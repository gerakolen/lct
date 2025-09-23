import json
import logging
from uuid import uuid4, UUID

from fastapi import APIRouter, Depends, Query, HTTPException

from sqlalchemy.orm import Session
from app.db import get_session
from app.model import TaskResponse, NewTaskRequest, ResultResponse, StatusResponse
from app.schema import Task, TaskStatus
from ..task import process_task


logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["task"],
    responses={404: {"description": "Not found"}},
)


@router.post("/new", response_model=TaskResponse)
def start_task(
    request: NewTaskRequest,
    session: Session = Depends(get_session),
):
    task_id = str(uuid4())
    new_task = Task(id=task_id, status=TaskStatus.PENDING)
    with session.begin():
        session.add(new_task)

    # Start Celery task asynchronously
    process_task.delay(task_id, request.model_dump())
    return {"taskid": task_id}


@router.get("/status", response_model=StatusResponse)
def get_status(
    task_id: UUID = Query(..., alias="task_id"),
    session: Session = Depends(get_session),
):
    task = session.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"status": task.status}


@router.get("/getresult", response_model=ResultResponse)
def get_result(
    task_id: UUID = Query(..., alias="task_id"),
    session: Session = Depends(get_session),
):
    task = session.query(Task).filter(Task.id == task_id).first()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status != "DONE":
        raise HTTPException(status_code=400, detail="Task not done yet")
    task_result = json.loads(task.result)
    logger.info(f"Task result: {task_result}")
    response = ResultResponse(
        ddl=task_result.get("ddl", []),
        migrations=task_result.get("migrations", []),
        queries=task_result.get("queries", []),
    )
    return response
