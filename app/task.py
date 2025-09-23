import logging
import uuid
from typing import Dict, Any

from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from time import sleep

from sqlalchemy.orm import sessionmaker, Session

from app.config import LCTSettings, lct_settings
from app.db import create_engine_from_url
from app.schema import Task, TaskStatus

logger = logging.getLogger(__name__)


# --- Build Celery ---
def create_celery(settings: LCTSettings) -> Celery:
    app = Celery(
        "task",
        broker=settings.queue.broker_url,
        backend=settings.queue.result_backend,
    )
    app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        task_always_eager=False,
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        # task_time_limit=20,
    )
    return app


celery_app = create_celery(lct_settings)

# --- Per-worker DB lifecycle ---
_engine = None
_SessionLocal: sessionmaker | None = None


@worker_process_init.connect
def _on_worker_boot(**_kwargs):
    """Each worker process gets its own Engine/SessionLocal."""
    global _engine, _SessionLocal
    _engine = create_engine_from_url(
        lct_settings.db.url
    )  # separate from FastAPI engine
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)
    logger.info("Worker DB engine initialized.")


@worker_process_shutdown.connect
def _on_worker_shutdown(**_kwargs):
    global _engine
    if _engine is not None:
        _engine.dispose()
        logger.info("Worker DB engine disposed.")


def _session() -> Session:
    assert _SessionLocal is not None, "Worker sessionmaker not initialized"
    return _SessionLocal()


# --- Business logic  ---
def _do_work(_payload: Dict[str, Any]) -> Dict[str, Any]:
    sleep(2)
    # return {"ok": True, "echo": payload, "meta": {"tokens_used": 0}}
    return {"ok": True, "meta": {"tokens_used": 0}}


@celery_app.task(
    bind=True,
    name="app.task.process_task",
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 5},
)
def process_task(self, task_id: str, payload: Dict[str, Any]) -> None:
    """
    1) mark task as running
    2) run work
    3) mark complete + persist result
    4) on any error: mark failed (+error)
    """
    # Validate UUID shape early
    try:
        uuid.UUID(task_id)
    except Exception:
        logger.error("Invalid task_id passed to process_task: %s", task_id)
        return

    # 1) Mark running
    with _session() as s:
        db_task = s.get(Task, task_id)
        if not db_task:
            logger.error("Task not found: %s", task_id)
            return
        db_task.status = TaskStatus.RUNNING
        db_task.error = None
        s.commit()

    # 2) Interact with Trino & LLM
    try:
        result = _do_work(payload)
    except Exception as e:
        # 3b) Mark failed on exception
        with _session() as s:
            db_task = s.get(Task, task_id)
            if db_task:
                db_task.status = TaskStatus.FAILED
                db_task.error = f"{type(e).__name__}: {e}"
                s.commit()
        logger.exception("Task %s failed", task_id)
        raise

    # 3a) Mark complete with result
    with _session() as s:
        db_task = s.get(Task, task_id)
        if db_task:
            db_task.status = TaskStatus.COMPLETE
            db_task.result = result
            db_task.error = None
            s.commit()