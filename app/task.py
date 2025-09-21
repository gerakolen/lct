from celery import Celery
from time import sleep
from uuid import UUID
from sqlalchemy.orm import Session
from app.schema import Task
from app.db import get_db
import json

app = Celery("tasks", broker="redis://localhost:6379/0")


@app.task
def process_task(task_id: str, request_data: dict):
    db: Session = next(get_db())
    try:
        # Simulate timeout check (Celery can handle timeouts natively if needed)
        sleep(10)  # Replace with actual processing time

        # Dummy result
        result = {
            "ddl": [{"statement": "CREATE TABLE new_t1 (...)"}],
            "migrations": [{"statement": "INSERT INTO new_t1 SELECT * FROM old_t1"}],
            "queries": [
                {"queryid": str(q["queryid"]), "query": q["query"] + " -- updated"}
                for q in request_data["queries"]
            ],
        }

        task = db.query(Task).filter(Task.id == UUID(task_id)).first()
        task.status = "DONE"
        task.result = json.dumps(result)
        db.commit()
    except Exception as e:
        task = db.query(Task).filter(Task.id == UUID(task_id)).first()
        task.status = "FAILED"
        db.commit()
        raise e
