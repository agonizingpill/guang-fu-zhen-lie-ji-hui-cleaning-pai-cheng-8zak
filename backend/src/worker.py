import os,time,socket,traceback
from pathlib import Path
import redis
from .database import Session
from .models import AssessmentJob
from .assessment_service import QUEUE,claim,compute_result,confirm_computation,write_evidence,finalize,fail_claimed,recover
queue=redis.Redis.from_url(os.getenv('REDIS_URL','redis://redis:6379/0'),decode_responses=True)
ME=f'{socket.gethostname()}:{os.getpid()}'
last=0.0
while True:
    Path('/tmp/solar-heartbeat').touch()
    if time.monotonic()-last>5:
        try:
            with Session() as db:recover(db,queue)
        except Exception:traceback.print_exc()
        last=time.monotonic()
    message=queue.blpop(QUEUE,timeout=3)
    if not message:continue
    key=int(message[1])
    try:
        with Session() as db:
            job=claim(db,key,ME)
            if not job:continue
            attempt,snapshot=job.attempt,job.input_snapshot
        try:result=compute_result(snapshot)
        except Exception as error:
            traceback.print_exc()
            with Session() as db:fail_claimed(db,key,ME,attempt,str(error))
            continue
        with Session() as db:
            if not confirm_computation(db,key,ME,attempt,result):continue
        try:write_evidence(result)
        except OSError:
            traceback.print_exc();continue  # 证据待补，恢复循环复用已确认计算补齐
        with Session() as db:finalize(db,db.get(AssessmentJob,key))
    except Exception:traceback.print_exc()
