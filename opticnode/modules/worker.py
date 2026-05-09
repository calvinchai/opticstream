from datetime import datetime, timedelta

from prefect.deployments import run_deployment
from pydantic import BaseModel, Field
from rq import Queue
import redis

from opticstream.state.lsm_project_state import LSMStripId

conn = redis.Redis(host="localhost", port=6379, db=0)
q = Queue("process-queue",connection=conn)
allowed_window = timedelta(minutes=10)
deployment_name = "process_strip"

class StripTask(BaseModel):
    lsm_strip_id: LSMStripId
    timestamp: datetime = Field(default_factory=datetime.now)
    strip_path: str

def publish(strip_task: StripTask):
    q.enqueue(process, strip_task.model_dump())

def send_to_backlog_queue(strip_task: StripTask):
    q=Queue("backlog-queue",connection=conn)
    q.enqueue(process, strip_task.model_dump())

def process(payload):
    task = StripTask.model_validate(payload)
    if task.timestamp < datetime.now() - allowed_window:
        send_to_backlog_queue(task)
        return

    start_deployment()

def process_backlog(payload):
    task = StripTask.model_validate(payload)
    start_deployment()

def start_deployment():
    run_deployment(
        name=deployment_name,
        parameters={"data": data}
    )
