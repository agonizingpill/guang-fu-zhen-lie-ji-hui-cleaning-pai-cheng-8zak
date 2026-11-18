# 估损作业可靠性回归：找回挂起、断点恢复、幂等重试、快照口径、补证、栅栏与过期作废。
# 运行：cd /workspace/test-artifacts/gb-1024/v4 && python3 -m pytest test_assessment_recovery.py -q
import os,sys,json,shutil
from datetime import datetime,timedelta
from pathlib import Path

BACKEND=Path('/workspace/backend')
sys.path.insert(0,str(BACKEND))
WORKDIR=Path('/tmp/gb1024-v4')
shutil.rmtree(WORKDIR,ignore_errors=True)
WORKDIR.mkdir(parents=True)
os.environ['DATABASE_URL']=f'sqlite:///{WORKDIR}/test.db?check_same_thread=false'
os.environ['REDIS_URL']='redis://fakeredis/0'
os.environ['ARTIFACT_DIR']=str(WORKDIR/'artifacts')
os.environ['SEED_DEMO']='false'
os.environ['JWT_SECRET']='test-secret'

import fakeredis,redis
SERVER=fakeredis.FakeServer()
redis.Redis.from_url=classmethod(lambda cls,*a,**k:fakeredis.FakeRedis(server=SERVER,decode_responses=True))

import sqlalchemy
_create_engine=sqlalchemy.create_engine
def _sqlite_friendly(url,*a,**k):
    if str(url).startswith('sqlite'):k.pop('isolation_level',None)  # READ COMMITTED 仅 MySQL 需要
    return _create_engine(url,*a,**k)
sqlalchemy.create_engine=_sqlite_friendly

import pytest
from sqlalchemy import select,update,func
from fastapi.testclient import TestClient
from src.app import app
from src.database import Session,engine,Base
from src import models
from src.auth import hash_password
from src.assessment_service import (QUEUE,claim,compute_result,confirm_computation,write_evidence,
    finalize,fail_claimed,recover,artifact_path,LEASE_SECONDS,GRACE_SECONDS,ARTIFACT_DIR)

Base.metadata.create_all(engine)
Q=fakeredis.FakeRedis(server=SERVER,decode_responses=True)

@pytest.fixture(autouse=True)
def clean_state():
    with engine.begin() as c:
        for table in reversed(Base.metadata.sorted_tables):c.execute(table.delete())
    Q.flushall()
    shutil.rmtree(ARTIFACT_DIR,ignore_errors=True)
    with Session() as db:
        for role in ['admin','planner','viewer']:
            db.add(models.User(email=f'{role}@qingling.test',password=hash_password('Solar2026!'),role=role))
        db.add(models.ArrayBlock(code='SL-T01',name='测试一号',sector='南坡',capacity_kw=150,slope=23,area_m2=780,water_liters=1500))
        db.commit()
    yield

@pytest.fixture()
def client():
    with TestClient(app) as c:yield c

def auth(client,email):
    r=client.post('/auth/login',json={'email':email,'password':'Solar2026!'})
    assert r.status_code==200,r.text
    return {'Authorization':'Bearer '+r.json()['token']}

def add_survey(client,code,irr=800,temp=35,dust=3.2,power=88):
    h=auth(client,'planner@qingling.test')
    r=client.post('/surveys',headers=h,json={'code':code,'block_id':1,'irradiance':irr,'module_temp':temp,'rain_days':18,'dust':dust,'observed_kw':power})
    assert r.status_code==200,r.text
    return r.json()['id']

def assess(client,key):
    r=client.post(f'/surveys/{key}/assess',headers=auth(client,'planner@qingling.test'))
    assert r.status_code==200,r.text
    return key

def the_job(survey_id):
    with Session() as db:return db.scalar(select(models.AssessmentJob).where(models.AssessmentJob.survey_id==survey_id))

def the_survey(survey_id):
    with Session() as db:return db.get(models.SoilingSurvey,survey_id)

def worker_cycle(handler):
    msg=Q.blpop(QUEUE,timeout=1)
    assert msg,'队列里应有待计算作业'
    key=int(msg[1])
    with Session() as db:
        job=claim(db,key,handler)
        assert job,f'{handler} 应认领成功'
        attempt,snapshot=job.attempt,job.input_snapshot
    result=compute_result(snapshot)
    with Session() as db:assert confirm_computation(db,key,handler,attempt,result)
    write_evidence(result)
    with Session() as db:outcome=finalize(db,db.get(models.AssessmentJob,key))
    return result,outcome

def test_accept_snapshots_inputs_and_queues(client):
    sid=assess(client,add_survey(client,'SV-T-001'))
    survey,the=the_survey(sid),the_job(sid)
    assert survey.state=='evaluating'
    assert the.state=='queued' and the.attempt==1 and the.handler is None
    snap=the.input_snapshot
    assert (snap['irradiance'],snap['module_temp'],snap['observed_kw'],snap['dust'])==(800,35,88,3.2)
    assert snap['capacity_kw']==150 and snap['slope']==23 and snap['area_m2']==780
    assert len(snap['reference_sha256'])==64
    assert Q.lrange(QUEUE,0,-1)==[str(the.id)]
    jobs=client.get('/assessment-jobs',headers=auth(client,'viewer@qingling.test')).json()
    assert len(jobs)==1 and jobs[0]['state']=='queued'

def test_recover_requeues_accepted_but_unsent(client):
    sid=assess(client,add_survey(client,'SV-T-002'))
    job=the_job(sid)
    Q.flushall()  # 送算前断连：队列消息丢失
    with Session() as db:
        recover(db,Q)  # 宽限期内不补投
        assert Q.llen(QUEUE)==0
        recover(db,Q,now=datetime.utcnow()+timedelta(seconds=GRACE_SECONDS+5))
    assert Q.lrange(QUEUE,0,-1)==[str(job.id)]
    result,outcome=worker_cycle('w-recover')
    assert outcome=='done' and the_survey(sid).state=='assessed'

def test_full_cycle_values_evidence_and_holdout(client):
    sid=assess(client,add_survey(client,'SV-T-003'))
    result,outcome=worker_cycle('w-1')
    assert outcome=='done'
    survey=the_survey(sid)
    assert survey.state=='assessed'
    assert survey.clean_kw==result['clean_kw'] and survey.predicted_loss==result['loss_kw']
    assert survey.recommended_water==round(780*.6*(1+.1*3.2),1)
    evidence=survey.model_evidence
    assert evidence['train_mae_kw']!=evidence['holdout_mae_kw']  # 独立留出误差不混同
    assert evidence['test_rows']==40 and evidence['random_state']==42
    payload=json.loads(artifact_path(sid).read_text())
    assert payload['clean_kw']==survey.clean_kw and payload['inputs']['observed_kw']==88
    assert the_job(sid).state=='done'

def test_interrupted_computation_recovered(client):
    sid=assess(client,add_survey(client,'SV-T-004'))
    key=int(Q.blpop(QUEUE,timeout=1)[1])
    with Session() as db:
        job=claim(db,key,'w-crash');assert job
        db.execute(update(models.AssessmentJob).where(models.AssessmentJob.id==key).values(heartbeat_at=datetime.utcnow()-timedelta(seconds=LEASE_SECONDS+10)))
        db.commit()  # 模拟计算中退出：心跳停摆
    with Session() as db:recover(db,Q)
    job=the_job(sid)
    assert job.state=='queued' and job.attempt==2 and job.handler is None
    assert str(job.id) in Q.lrange(QUEUE,0,-1)
    result,outcome=worker_cycle('w-new')
    assert outcome=='done' and the_survey(sid).state=='assessed'

def test_stale_handler_cannot_overwrite_results_or_files(client):
    sid=assess(client,add_survey(client,'SV-T-005'))
    key=int(Q.blpop(QUEUE,timeout=1)[1])
    with Session() as db:
        job=claim(db,key,'w-old');assert job
        db.execute(update(models.AssessmentJob).where(models.AssessmentJob.id==key).values(heartbeat_at=datetime.utcnow()-timedelta(seconds=LEASE_SECONDS+10)))
        db.commit()
    with Session() as db:recover(db,Q)  # 新办理者接手
    result,outcome=worker_cycle('w-current')
    assert outcome=='done'
    fake=dict(result,clean_kw=1.0,loss_kw=999.0)
    with Session() as db:
        assert not confirm_computation(db,key,'w-old',1,fake)  # 旧办理者晚回，写不进去
        fail_claimed(db,key,'w-old',1,'旧办理者误报')            # 也不得改写为失败
    survey=the_survey(sid)
    assert survey.state=='assessed' and survey.clean_kw==result['clean_kw']
    assert json.loads(artifact_path(sid).read_text())['clean_kw']==result['clean_kw']
    assert the_job(sid).state=='done'

def test_retry_same_assessment_adds_no_new_results(client):
    sid=assess(client,add_survey(client,'SV-T-006'))
    result,_=worker_cycle('w-1')
    Q.rpush(QUEUE,str(the_job(sid).id))  # 重复消息
    key=int(Q.blpop(QUEUE,timeout=1)[1])
    with Session() as db:assert claim(db,key,'w-dup') is None  # 已完成，认领落空
    with Session() as db:assert finalize(db,db.get(models.AssessmentJob,the_job(sid).id))=='done'
    r=client.post(f'/surveys/{sid}/assess',headers=auth(client,'planner@qingling.test'))
    assert r.status_code==409  # 已评估不可重复受理
    with Session() as db:
        count=db.scalar(select(func.count()).select_from(models.AssessmentJob).where(models.AssessmentJob.survey_id==sid))
    assert count==1
    assert len(list(ARTIFACT_DIR.glob(f'survey-{sid}.json')))==1
    assert the_survey(sid).clean_kw==result['clean_kw']

def test_retry_uses_acceptance_snapshot(client):
    sid=assess(client,add_survey(client,'SV-T-007'))
    with Session() as db:  # 受理后读数被改写（直改库模拟），重算仍用受理快照
        db.execute(update(models.SoilingSurvey).where(models.SoilingSurvey.id==sid).values(irradiance=950,observed_kw=40,dust=9))
        db.commit()
    result,outcome=worker_cycle('w-snap')
    assert outcome=='done'
    survey=the_survey(sid)
    assert survey.predicted_loss==result['loss_kw']
    assert result['inputs']['irradiance']==800 and result['inputs']['observed_kw']==88 and result['inputs']['dust']==3.2
    assert survey.clean_kw==result['clean_kw']  # 与受理时 800W/m²、88kW 口径一致

def test_evidence_failure_backfilled_from_confirmed(client):
    sid=assess(client,add_survey(client,'SV-T-008'))
    key=int(Q.blpop(QUEUE,timeout=1)[1])
    with Session() as db:
        job=claim(db,key,'w-1');attempt,snapshot=job.attempt,job.input_snapshot
    result=compute_result(snapshot)
    with Session() as db:assert confirm_computation(db,key,'w-1',attempt,result)
    os.chmod(ARTIFACT_DIR.parent,0o555)  # 证据卷不可写
    try:
        with pytest.raises(OSError):write_evidence(result)
        with Session() as db:recover(db,Q)
        assert the_job(sid).state=='evidence_pending'  # 补证中
        assert the_survey(sid).state=='evaluating'     # 证据未齐备不放行
        r=client.post('/missions',headers=auth(client,'planner@qingling.test'),json={'survey_id':sid,'water_liters':100})
        assert r.status_code==409                       # 不可编制清洗计划
    finally:os.chmod(ARTIFACT_DIR.parent,0o755)
    with Session() as db:recover(db,Q)  # 复用已确认计算补齐文件
    assert the_job(sid).state=='done'
    assert the_survey(sid).state=='assessed'
    payload=json.loads(artifact_path(sid).read_text())
    assert payload==the_job(sid).result_snapshot
    r=client.post('/missions',headers=auth(client,'planner@qingling.test'),json={'survey_id':sid,'water_liters':100})
    assert r.status_code==200

def wash_cycle(client,survey_id,water=100):
    h_planner,h_admin=auth(client,'planner@qingling.test'),auth(client,'admin@qingling.test')
    r=client.post('/missions',headers=h_planner,json={'survey_id':survey_id,'water_liters':water});assert r.status_code==200,r.text
    mid=r.json()['id']
    r=client.post('/crews',headers=h_admin,json={'code':'CREW-T','max_daily_minutes':480});assert r.status_code==200,r.text
    crew=r.json()['id']
    r=client.post('/permits',headers=h_planner,json={'mission_id':mid,'crew_id':crew,'work_date':'2026-11-18','minutes':120,'wind_mps':5,'isolation':True});assert r.status_code==200,r.text
    r=client.post(f"/permits/{r.json()['id']}/sign",headers=h_admin);assert r.status_code==200,r.text
    r=client.post(f'/missions/{mid}/dispatch',headers=h_admin);assert r.status_code==200,r.text
    r=client.post(f'/missions/{mid}/retest',headers=h_planner,json={'post_kw':110,'post_irradiance':800});assert r.status_code==200,r.text
    r=client.post(f'/missions/{mid}/accept',headers=h_admin);assert r.status_code==200,r.text
    return mid

def test_expired_survey_recompute_never_restored(client):
    sid_a=assess(client,add_survey(client,'SV-T-009A'))
    worker_cycle('w-a')
    sid_b=assess(client,add_survey(client,'SV-T-009B',power=90))  # B 在计算队列中
    sid_c=assess(client,add_survey(client,'SV-T-009C',power=91))
    key_c=int(Q.blpop(QUEUE,timeout=1)[1])  # C 被认领、数值已确认、等补证
    with Session() as db:
        job=claim(db,key_c,'w-c');att_c,snap_c=job.attempt,job.input_snapshot
    result_c=compute_result(snap_c)
    with Session() as db:assert confirm_computation(db,key_c,'w-c',att_c,result_c)
    wash_cycle(client,sid_a)  # 清洗验收使同阵列旧巡测过期
    assert the_survey(sid_b).state=='stale' and the_survey(sid_c).state=='stale'
    with Session() as db:recover(db,Q)
    job_b,job_c=the_job(sid_b),the_job(sid_c)
    assert job_b.state=='failed' and '过期' in job_b.error
    assert job_c.state=='failed' and '过期' in job_c.error  # 补算不恢复排程资格
    assert the_survey(sid_b).state=='stale' and the_survey(sid_c).state=='stale'
    assert not artifact_path(sid_c).exists()  # 过期巡测不补写证据
    with Session() as db:assert not confirm_computation(db,key_c,'w-c',att_c,result_c)
    for sid in (sid_b,sid_c):
        r=client.post('/missions',headers=auth(client,'planner@qingling.test'),json={'survey_id':sid,'water_liters':100})
        assert r.status_code==409

def test_compute_failure_reason_and_retry_resets_same_job(client):
    sid=assess(client,add_survey(client,'SV-T-010'))
    job=the_job(sid)
    with Session() as db:  # 破坏快照使计算必失败
        snap=dict(job.input_snapshot);snap['irradiance']=200
        db.execute(update(models.AssessmentJob).where(models.AssessmentJob.id==job.id).values(input_snapshot=snap))
        db.commit()
    key=int(Q.blpop(QUEUE,timeout=1)[1])
    with Session() as db:
        claimed=claim(db,key,'w-1');attempt,snapshot=claimed.attempt,claimed.input_snapshot
    with pytest.raises(ValueError):compute_result(snapshot)
    with Session() as db:fail_claimed(db,key,'w-1',attempt,'辐照低于350 W/m²，请在稳定光照下补测')
    job=the_job(sid)
    assert job.state=='failed' and '辐照' in job.error
    survey=the_survey(sid)
    assert survey.state=='blocked' and '辐照' in survey.model_evidence['error']
    jobs=client.get('/assessment-jobs',headers=auth(client,'viewer@qingling.test')).json()
    assert jobs[0]['state']=='failed' and '辐照' in jobs[0]['error']
    assess(client,sid)  # 失败后重新受理：复位原作业，不新增行
    job2=the_job(sid)
    assert job2.id==job.id and job2.attempt==attempt+1 and job2.state=='queued'
    assert job2.input_snapshot['irradiance']==800
    result,outcome=worker_cycle('w-2')
    assert outcome=='done' and the_survey(sid).state=='assessed'
