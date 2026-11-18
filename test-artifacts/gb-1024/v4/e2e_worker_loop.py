# 端到端冒烟：真实 worker 循环 + API，验证受理→计算→补证→放行全链路。
import os,sys,time,shutil,threading
from pathlib import Path
BACKEND=Path('/workspace/backend');sys.path.insert(0,str(BACKEND))
WORKDIR=Path('/tmp/gb1024-v4-e2e');shutil.rmtree(WORKDIR,ignore_errors=True);WORKDIR.mkdir(parents=True)
os.environ['DATABASE_URL']=f'sqlite:///{WORKDIR}/e2e.db?check_same_thread=false'
os.environ['REDIS_URL']='redis://fakeredis/0'
os.environ['ARTIFACT_DIR']=str(WORKDIR/'artifacts')
os.environ['SEED_DEMO']='false';os.environ['JWT_SECRET']='test-secret'
import fakeredis,redis,sqlalchemy
SERVER=fakeredis.FakeServer()
redis.Redis.from_url=classmethod(lambda cls,*a,**k:fakeredis.FakeRedis(server=SERVER,decode_responses=True))
_create=sqlalchemy.create_engine
sqlalchemy.create_engine=lambda url,*a,**k:_create(url,*a,**{x:y for x,y in k.items() if x!='isolation_level'}) if str(url).startswith('sqlite') else _create(url,*a,**k)
from fastapi.testclient import TestClient
from src.app import app
from src.database import Session,engine,Base
from src import models
from src.auth import hash_password
Base.metadata.create_all(engine)
threading.Thread(target=lambda:__import__('src.worker',fromlist=['worker']),daemon=True).start()  # 真实 worker 循环
time.sleep(0.5)

with Session() as db:
    db.add(models.User(email='planner@qingling.test',password=hash_password('Solar2026!'),role='planner'))
    db.add(models.ArrayBlock(code='SL-E01',name='端到端一号',sector='南坡',capacity_kw=150,slope=23,area_m2=780,water_liters=1500))
    db.commit()

with TestClient(app) as client:
    token=client.post('/auth/login',json={'email':'planner@qingling.test','password':'Solar2026!'}).json()['token']
    h={'Authorization':'Bearer '+token}
    sid=client.post('/surveys',headers=h,json={'code':'SV-E2E-01','block_id':1,'irradiance':800,'module_temp':35,'rain_days':18,'dust':3.2,'observed_kw':88}).json()['id']
    client.post(f'/surveys/{sid}/assess',headers=h)
    deadline=time.time()+30
    while time.time()<deadline:
        survey=client.get(f'/surveys/{sid}',headers=h).json()
        if survey['state']=='assessed':break
        time.sleep(0.5)
    else:raise SystemExit('FAIL: worker 未完成估损')
    jobs=client.get('/assessment-jobs',headers=h).json()
    assert jobs[0]['state']=='done',jobs
    artifact=WORKDIR/'artifacts'/f'survey-{sid}.json'
    assert artifact.exists(),'证据文件缺失'
    import json;payload=json.loads(artifact.read_text())
    assert payload['clean_kw']==survey['clean_kw'] and payload['inputs']['observed_kw']==88
    assert survey['model_evidence']['holdout_mae_kw']!=survey['model_evidence']['train_mae_kw']
    print(f"E2E OK: survey={survey['state']} clean={survey['clean_kw']:.3f}kW loss={survey['predicted_loss']:.3f}kW job={jobs[0]['state']} artifact={artifact.name}",flush=True)
    sys.stdout.flush();sys.stderr.flush()
os._exit(0)  # worker 线程为无限循环，直接退出
