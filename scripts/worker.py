# app/worker.py (фрагмент)
from celery import Celery
from app.analysis.sql_static import build_context_pack
from app.storage import TaskStorage  # предположим, у вас есть обертка над SQLite
from app.schemas import NewTaskPayload  # ваш Pydantic под вход /new

celery_app = Celery("tasks")

@celery_app.task(name="analyze_and_prepare_context")
def analyze_and_prepare_context(task_id: str):
    storage = TaskStorage()
    task = storage.load_task(task_id)  # содержит payload с ddl, queries, url (jdbc)
    try:
        payload = NewTaskPayload.model_validate(task.payload)
        context_pack = build_context_pack(payload.ddl, payload.queries)

        # Обновим задачу промежуточным результатом: meta.context_pack
        storage.update_task_partial(task_id, status="RUNNING", meta={"context_pack": context_pack})

        # Дальше по пайплайну у вас пойдут:
        # - сэмпл EXPLAIN для 5–10 топ-запросов (Шаг B)
        # - генерация кандидатов CTAS/VIEW (Шаг C)
        # - LLM-сборка итогового JSON (Шаг D)
        # Здесь пока завершаем шаг A
        return {"ok": True, "meta": {"context_pack_ready": True, "stats": context_pack.get("queries_overview", {})}}

    except Exception as e:
        storage.update_task_partial(task_id, status="FAILED", meta={"error": str(e)})
        raise
