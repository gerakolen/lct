import logging
import uuid
from typing import Dict, Any

from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
from celery.signals import worker_process_init, worker_process_shutdown

from sqlalchemy.orm import sessionmaker, Session

from app.analyze.sql_static import build_context_pack
from app.client.trino_client import extract_connection_details
from app.client.yandex_client import call_yandex
from app.config import LCTSettings, lct_settings
from app.db import create_engine_from_url
from app.schema import Task, TaskStatus

logger = logging.getLogger(__name__)

TIME_SAFETY_MARGIN_SECS = 10
SLEEP_INTERVAL_SECS = 5
MOCK_TASK_PROCESSING_TICS: int = 2
# MOCK_TASK_PROCESSING_TICS: int = 500


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
        task_soft_time_limit=settings.queue.task_time_limit_secs,
        task_time_limit=settings.queue.task_time_limit_secs + TIME_SAFETY_MARGIN_SECS,
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


def _do_work(payload: Dict[str, Any]) -> Dict[str, Any]:
    queries = payload.get("queries", "")
    ddl = payload.get("ddl", [])

    context_pack = build_context_pack(ddl=ddl, queries=queries)
    requirements = {
        'catalog': context_pack['default_catalog'],
        'source_schema': context_pack['default_schema'],
        'target_schema': 'new_schema'
    }
    result = call_yandex(context_pack, payload, requirements=requirements)
    return result

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
    except (SoftTimeLimitExceeded, TimeLimitExceeded):
        logger.error(f"Time limit exceeded for: {task_id}")
        with _session() as s:
            t = s.get(Task, task_id)
            if t:
                t.status = TaskStatus.FAILED
                t.error = "Timeout exceeded"
                s.commit()
        return

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
