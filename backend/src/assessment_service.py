import json,os
from datetime import datetime,timedelta
from pathlib import Path
from sqlalchemy import select,update
from .models import SoilingSurvey,AssessmentJob,Audit
from .power_model import train_reference,estimate_values

QUEUE='solar:assessment'
GRACE_SECONDS=5       # 受理后补投宽限，避免与正常送算竞争
LEASE_SECONDS=60      # 计算租约；心跳超时视为计算中断
ARTIFACT_DIR=Path(os.getenv('ARTIFACT_DIR','/artifacts'))

def snapshot_inputs(survey,block):
    # 受理时固定读数与清洁功率参考，重试一律使用这份快照
    reference=train_reference()[1]
    return {'survey_id':survey.id,'survey_code':survey.code,'block_id':block.id,'block_code':block.code,'area_m2':block.area_m2,'capacity_kw':survey.capacity_kw,'slope':survey.slope,'irradiance':survey.irradiance,'module_temp':survey.module_temp,'rain_days':survey.rain_days,'dust':survey.dust,'observed_kw':survey.observed_kw,'reference_sha256':reference['sha256']}

def accept_assessment(db,survey,block,user):
    # 同一巡测只保留一条作业：重试复位原行并换发尝试号，不新增结果
    now=datetime.utcnow();survey.state='evaluating';snapshot=snapshot_inputs(survey,block)
    job=db.scalar(select(AssessmentJob).where(AssessmentJob.survey_id==survey.id))
    if job:
        job.state='queued';job.attempt+=1;job.handler=None;job.input_snapshot=snapshot;job.result_snapshot=None;job.error=None
        job.queued_at=now;job.started_at=None;job.heartbeat_at=None;job.finished_at=None
    else:
        job=AssessmentJob(survey_id=survey.id,state='queued',attempt=1,input_snapshot=snapshot,queued_at=now)
        db.add(job);db.flush()
    db.add(Audit(actor=user.email,action=f'评估积灰 {survey.code}，第{job.attempt}次受理'))
    return job

def claim(db,job_id,handler):
    # 只有待计算作业能被认领；并发弹窗下只有一个办理者认领成功
    now=datetime.utcnow()
    res=db.execute(update(AssessmentJob).where(AssessmentJob.id==job_id,AssessmentJob.state=='queued').values(state='running',handler=handler,started_at=now,heartbeat_at=now,error=None))
    db.commit()
    return db.get(AssessmentJob,job_id) if res.rowcount==1 else None

def compute_result(snapshot):
    clean,loss,evidence=estimate_values(snapshot)
    water=round(snapshot['area_m2']*.6*(1+.1*snapshot['dust']),1)
    return {'survey_id':snapshot['survey_id'],'survey_code':snapshot['survey_code'],'clean_kw':clean,'loss_kw':loss,'recommended_water':water,'model':evidence,'inputs':snapshot}

def confirm_computation(db,job_id,handler,attempt,result):
    # 旧办理者晚回时尝试号/办理者已变更，条件更新落空，结果与文件都不得改写
    now=datetime.utcnow()
    res=db.execute(update(AssessmentJob).where(AssessmentJob.id==job_id,AssessmentJob.state=='running',AssessmentJob.handler==handler,AssessmentJob.attempt==attempt).values(state='evidence_pending',result_snapshot=result,heartbeat_at=now,finished_at=now,error=None))
    db.commit()
    return res.rowcount==1

def artifact_path(survey_id):return ARTIFACT_DIR/f'survey-{survey_id}.json'

def write_evidence(result):
    # 临时文件+原子改名：崩溃只留半成品临时文件，正式文件不被截断
    ARTIFACT_DIR.mkdir(parents=True,exist_ok=True)
    tmp=ARTIFACT_DIR/f'.survey-{result["survey_id"]}.{os.getpid()}.tmp'
    tmp.write_text(json.dumps(result,ensure_ascii=False))
    os.replace(tmp,artifact_path(result['survey_id']))

def finalize(db,job):
    # 数值与证据文件齐备后才放行巡测；巡测已过期则作废，不恢复排程资格
    result=job.result_snapshot
    res=db.execute(update(SoilingSurvey).where(SoilingSurvey.id==job.survey_id,SoilingSurvey.state=='evaluating').values(clean_kw=result['clean_kw'],predicted_loss=result['loss_kw'],recommended_water=result['recommended_water'],model_evidence=result['model'],state='assessed'))
    if res.rowcount==1:
        db.execute(update(AssessmentJob).where(AssessmentJob.id==job.id,AssessmentJob.state=='evidence_pending').values(state='done'))
        db.commit();return 'done'
    db.rollback()
    survey=db.scalar(select(SoilingSurvey).where(SoilingSurvey.id==job.survey_id).execution_options(populate_existing=True))
    if survey and survey.state=='assessed':
        db.execute(update(AssessmentJob).where(AssessmentJob.id==job.id,AssessmentJob.state=='evidence_pending').values(state='done'))
        db.commit();return 'done'
    db.execute(update(AssessmentJob).where(AssessmentJob.id==job.id).values(state='failed',error='巡测已因清洗验收或排程变更过期，估损结果不恢复排程资格',finished_at=datetime.utcnow()))
    db.commit();return 'expired'

def fail_claimed(db,job_id,handler,attempt,reason):
    job=db.get(AssessmentJob,job_id)
    if not job:return
    res=db.execute(update(AssessmentJob).where(AssessmentJob.id==job_id,AssessmentJob.state=='running',AssessmentJob.handler==handler,AssessmentJob.attempt==attempt).values(state='failed',error=reason[:400],finished_at=datetime.utcnow()))
    if res.rowcount!=1:db.rollback();return
    db.execute(update(SoilingSurvey).where(SoilingSurvey.id==job.survey_id,SoilingSurvey.state=='evaluating').values(state='blocked',model_evidence={'error':reason}))
    db.commit()

def recover(db,queue,now=None):
    # 找回挂起作业：作废过期巡测、收回中断计算、补投未送算、补齐证据文件
    now=now or datetime.utcnow()
    active=db.scalars(select(AssessmentJob).where(AssessmentJob.state.in_(['queued','running']))).all()
    for job in active:
        survey=db.get(SoilingSurvey,job.survey_id)
        if not survey or survey.state!='evaluating':
            db.execute(update(AssessmentJob).where(AssessmentJob.id==job.id,AssessmentJob.state.in_(['queued','running'])).values(state='failed',error='巡测已因清洗验收过期，估损不再恢复排程资格',finished_at=now))
            db.commit()
    expired=db.scalars(select(AssessmentJob).where(AssessmentJob.state=='running',AssessmentJob.heartbeat_at<now-timedelta(seconds=LEASE_SECONDS))).all()
    for job in expired:
        res=db.execute(update(AssessmentJob).where(AssessmentJob.id==job.id,AssessmentJob.state=='running',AssessmentJob.heartbeat_at<now-timedelta(seconds=LEASE_SECONDS)).values(state='queued',attempt=job.attempt+1,handler=None,queued_at=now,heartbeat_at=None))
        if res.rowcount==1:
            db.commit();queue.rpush(QUEUE,str(job.id))
        else:db.rollback()
    queued=db.scalars(select(AssessmentJob).where(AssessmentJob.state=='queued')).all()
    if queued:
        inflight=set(queue.lrange(QUEUE,0,-1))
        for job in queued:
            if str(job.id) not in inflight and job.queued_at<now-timedelta(seconds=GRACE_SECONDS):
                queue.rpush(QUEUE,str(job.id))
    for job in db.scalars(select(AssessmentJob).where(AssessmentJob.state=='evidence_pending')).all():
        survey=db.get(SoilingSurvey,job.survey_id)
        if survey and survey.state=='evaluating':
            try:write_evidence(job.result_snapshot)
            except OSError:continue
        finalize(db,job)
