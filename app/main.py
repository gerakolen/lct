from typing import Dict

from fastapi import FastAPI, Depends, HTTPException, Query
from uuid import uuid4, UUID
from sqlalchemy.orm import Session
from app.model import NewTaskRequest, TaskResponse, StatusResponse, ResultResponse
from app.task import process_task
from app.db import get_db
from app.schema import Task
import json

app = FastAPI()


@app.post("/new", response_model=TaskResponse)
def start_task(request: NewTaskRequest, db: Session = Depends(get_db)):
    task_id = uuid4()
    new_task = Task(id=task_id)
    db.add(new_task)
    db.commit()

    print(f"Input Data: {request.model_dump()}")
    # Start Celery task asynchronously
    # process_task.delay(str(task_id), request.model_dump())

    # Set a timeout (e.g., using Celery's time_limit or a separate watcher)
    # For now, assume Celery handles it; you can add a scheduler to fail after 20 min

    return {"taskid": task_id}


@app.get("/status", response_model=StatusResponse)
def get_status(
    task_id: UUID = Query(..., alias="task_id"), db: Session = Depends(get_db)
):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"status": task.status}


@app.get("/getresult", response_model=ResultResponse)
def get_result(
    task_id: UUID = Query(..., alias="task_id"), db: Session = Depends(get_db)
):
    task = db.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status != "DONE":
        raise HTTPException(status_code=400, detail="Task not done yet")
    return json.loads(task.result)

@app.get("/")
def read_root() -> Dict[str, str]:
    return {"Hello": "World"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8998)
