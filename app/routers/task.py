import logging
import uuid
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Query, HTTPException

from sqlalchemy.orm import Session
from app.db import get_session
from app.model import (
    TaskResponse,
    NewTaskRequest,
    StatusResponse,
    TaskResultResponse,
    ExplainRequest,
    ExplainResponse,
    create_mock_response,
)
from app.schema import Task, TaskStatus
from ..client.trino_client import explain_analyze
from ..config import LCTSettings
from ..worker_task import process_task


logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["task"],
    responses={404: {"description": "Not found"}},
)


def sort_queries_by_runquantity(data: NewTaskRequest) -> NewTaskRequest:
    """
    Sorts the 'queries' list in the input dictionary by 'runquantity' in desc order.
    """
    data.queries = sorted(data.queries, key=lambda q: q.runquantity, reverse=True)
    return data


@router.post("/new", response_model=TaskResponse)
def start_task(
    request: NewTaskRequest,
    session: Session = Depends(get_session),
):
    task_id = str(uuid.uuid4())
    new_task = Task(id=task_id, status=TaskStatus.PENDING)
    with session.begin():
        session.add(new_task)

    # Start Celery task asynchronously
    sorted_by_quantity = sort_queries_by_runquantity(request)
    process_task.delay(task_id, sorted_by_quantity.model_dump())
    return {"taskid": task_id}


@router.get("/status", response_model=StatusResponse)
def get_status(
    task_id: UUID = Query(..., alias="task_id"), session: Session = Depends(get_session)
):
    task = session.get(Task, str(task_id))
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    # return {"taskid": str(task.id), "status": task.status.value, "error": task.error}
    return {"status": task.status}


@router.get("/getresult", response_model=TaskResultResponse)
def get_result(
    task_id: UUID = Query(..., alias="task_id"),
    session: Session = Depends(get_session),
):
    task_id_str = str(task_id)
    task = session.get(Task, task_id_str)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
        raise HTTPException(status_code=400, detail={"status": task.status.value})
    if task.status == TaskStatus.FAILED:
        raise HTTPException(
            status_code=400, detail={"status": task.status.value, "error": task.error}
        )
    return create_mock_response()


@router.post("/explain", response_model=ExplainResponse)
def explain(req: ExplainRequest, request: Request):
    settings: LCTSettings = request.app.state.settings
    trino_settings = settings.trino
    plan = explain_analyze(req.sql, trino_settings)
    logger.info(plan)
    return {"plan": plan}
