import logging
import uuid
from typing import Dict, Any

from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
from celery.signals import worker_process_init, worker_process_shutdown
from time import sleep

from sqlalchemy.orm import sessionmaker, Session

from app.client.trino_client import extract_connection_details
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


def mock_response_for_do_work() -> Dict[str, Any]:
    return {
        "ddl": [
            {
                "statement": "CREATE TABLE quests.public.h_author ( id integer, name varchar, created_at timestamp(6));"
            }
        ],
        "migrations": [
            {
                "statement": "INSERT INTO quests.public.h_author_new SELECT * FROM quests.public.h_author;"
            }
        ],
        "queries": [
            {
                "queryid": "3b1cc90f-d446-4592-becd-8c26efbabf56",
                "query": "WITH raw_data AS (SELECT ha.id AS author_id, ha.name AS author_name, ep.excursion_id, qp.quest_id, ei.amount AS exc_amount, qi.amount AS quest_amount FROM quests.public.h_author ha LEFT JOIN quests.public.l_excursion_author ea ON ha.id = ea.author_id LEFT JOIN quests.public.l_excursion_payment ep ON ea.excursion_id = ep.excursion_id LEFT JOIN quests.public.s_payment_info ei ON ep.payment_id = ei.payment_id LEFT JOIN quests.public.l_author_quest aq ON ha.id = aq.author_id LEFT JOIN quests.public.l_quest_payment qp ON aq.quest_id = qp.quest_id LEFT JOIN quests.public.s_payment_info qi ON qp.payment_id = qi.payment_id LIMIT 500000), dup AS (SELECT r.*, x.n AS mult FROM raw_data r CROSS JOIN (VALUES 1,2) AS x(n)), calc AS (SELECT author_id, author_name, COUNT(excursion_id) + COUNT(quest_id) AS total_sales, COALESCE(SUM(exc_amount),0) + COALESCE(SUM(quest_amount),0) AS total_revenue, ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY random()) AS rn, AVG(exc_amount) OVER (PARTITION BY author_id) AS avg_exc, MAX(quest_amount) OVER (PARTITION BY author_id) AS max_quest FROM dup GROUP BY author_id, author_name, excursion_id, quest_id, exc_amount, quest_amount) SELECT author_id, author_name, total_sales, total_revenue, avg_exc, max_quest FROM calc WHERE rn < 500 ORDER BY random();",
                "runquantity": 44,
                "executiontime": 25,
            }
        ],
    }


# --- Business logic  ---
def _do_work(payload: Dict[str, Any]) -> Dict[str, Any]:
    queries = payload.get("queries", "")
    ddl = payload.get("ddl", [])
    logger.info(f"Queries: {queries}, DDL: {ddl}")
    jdbc_url = payload.get("url", "")
    trino_settings = extract_connection_details(jdbc_url)
    logger.info(f"Trino settings: {trino_settings}")
    # comment out because we don't interact directly with trino now
    # mock_sql = "EXPLAIN ANALYZE SELECT t.* FROM system.runtime.queries t WHERE t.source = 'dbt-trino-1.8.0' LIMIT 10"
    # explain_result = explain_analyze(mock_sql, trino_settings)
    # logger.info(explain_result)

    result = {
        "default_catalog": "quests",
        "default_schema": "public",
        "ddl_tables": [],
        "queries_overview": {
            "total_queries": 11,
            "total_runquantity": 4484
        },
        "join_graph_edges": [
            [
            "quests.public.l_excursion_payment",
            "quests.public.l_payment_client",
            2679
            ],
            [
            "quests.public.h_client",
            "quests.public.l_payment_client",
            482
            ],
            [
            "quests.public.h_client",
            "quests.public.s_client_geo_info",
            482
            ],
            [
            "quests.public.l_payment_client",
            "quests.public.l_quest_payment",
            2667
            ],
            [
            "quests.public.l_payment_client",
            "quests.public.s_client_personal_info",
            2464
            ],
            [
            "quests.public.h_client",
            "quests.public.s_client_personal_info",
            176
            ],
            [
            "quests.public.s_client_geo_info",
            "quests.public.s_client_personal_info",
            88
            ],
            [
            "quests.public.l_excursion_payment",
            "quests.public.s_excursion_geo_info",
            12
            ],
            [
            "quests.public.h_author",
            "quests.public.l_excursion_author",
            44
            ],
            [
            "quests.public.l_excursion_author",
            "quests.public.l_excursion_payment",
            44
            ],
            [
            "quests.public.l_excursion_payment",
            "quests.public.s_payment_info",
            322
            ],
            [
            "quests.public.h_author",
            "quests.public.l_author_quest",
            44
            ],
            [
            "quests.public.l_author_quest",
            "quests.public.l_quest_payment",
            44
            ],
            [
            "quests.public.l_quest_payment",
            "quests.public.s_payment_info",
            322
            ],
            [
            "quests.public.l_payment_client",
            "quests.public.s_payment_info",
            66
            ],
            [
            "quests.public.l_excursion_category",
            "quests.public.l_excursion_payment",
            278
            ],
            [
            "quests.public.h_category",
            "quests.public.l_excursion_category",
            278
            ],
            [
            "quests.public.l_quest_category",
            "quests.public.l_quest_payment",
            278
            ],
            [
            "quests.public.h_category",
            "quests.public.l_quest_category",
            278
            ],
            [
            "quests.public.h_episode",
            "quests.public.s_episode_completion_info",
            559
            ],
            [
            "quests.public.h_episode",
            "quests.public.l_quest_episode",
            559
            ],
            [
            "quests.public.h_quest",
            "quests.public.l_quest_episode",
            559
            ],
            [
            "quests.public.l_payment_client",
            "quests.public.l_payment_promo",
            34
            ]
        ],
        "join_key_freq": {
            "quests.public.l_excursion_payment|quests.public.l_payment_client|payment_id|payment_id": 2679,
            "quests.public.h_client|quests.public.l_payment_client|id|client_id": 482,
            "quests.public.h_client|quests.public.s_client_geo_info|id|client_id": 482,
            "quests.public.l_payment_client|quests.public.l_quest_payment|payment_id|payment_id": 2667,
            "quests.public.l_payment_client|quests.public.s_client_personal_info|client_id|client_id": 2464,
            "quests.public.h_client|quests.public.s_client_personal_info|id|client_id": 176,
            "quests.public.s_client_geo_info|quests.public.s_client_personal_info|client_id|client_id": 88,
            "quests.public.l_excursion_payment|quests.public.s_excursion_geo_info|excursion_id|excursion_id": 12,
            "quests.public.h_author|quests.public.l_excursion_author|id|author_id": 44,
            "quests.public.l_excursion_author|quests.public.l_excursion_payment|excursion_id|excursion_id": 44,
            "quests.public.l_excursion_payment|quests.public.s_payment_info|payment_id|payment_id": 322,
            "quests.public.h_author|quests.public.l_author_quest|id|author_id": 44,
            "quests.public.l_author_quest|quests.public.l_quest_payment|quest_id|quest_id": 44,
            "quests.public.l_quest_payment|quests.public.s_payment_info|payment_id|payment_id": 322,
            "quests.public.l_payment_client|quests.public.s_payment_info|payment_id|payment_id": 66,
            "quests.public.l_excursion_category|quests.public.l_excursion_payment|excursion_id|excursion_id": 278,
            "quests.public.h_category|quests.public.l_excursion_category|id|category_id": 278,
            "quests.public.l_quest_category|quests.public.l_quest_payment|quest_id|quest_id": 278,
            "quests.public.h_category|quests.public.l_quest_category|id|category_id": 278,
            "quests.public.h_episode|quests.public.s_episode_completion_info|id|episode_id": 559,
            "quests.public.h_episode|quests.public.l_quest_episode|id|episode_id": 559,
            "quests.public.h_quest|quests.public.l_quest_episode|id|quest_id": 559,
            "quests.public.l_payment_client|quests.public.l_payment_promo|payment_id|payment_id": 34
        },
        "table_scan_freq": {
            "quests.public.l_excursion_payment": 3001,
            "quests.public.l_payment_client": 2992,
            "quests.public.h_client": 746,
            "quests.public.s_client_geo_info": 570,
            "quests.public.l_quest_payment": 2989,
            "quests.public.s_client_personal_info": 3310,
            "quests.public.calc": 132,
            "quests.public.big": 264,
            "quests.public.base": 352,
            "quests.public.quarterly_sales": 12,
            "quests.public.s_excursion_geo_info": 12,
            "quests.public.sales_by_period": 12,
            "quests.public.h_author": 44,
            "quests.public.l_excursion_author": 44,
            "quests.public.s_payment_info": 710,
            "quests.public.l_author_quest": 44,
            "quests.public.raw_data": 44,
            "quests.public.dup": 44,
            "quests.public.l_excursion_category": 278,
            "quests.public.h_category": 556,
            "quests.public.l_quest_category": 278,
            "quests.public.h_episode": 559,
            "quests.public.s_episode_completion_info": 559,
            "quests.public.l_quest_episode": 559,
            "quests.public.h_quest": 559,
            "quests.public.l_payment_promo": 34
        },
        "table_scan_query_freq": {
            "quests.public.l_payment_client": 2757,
            "quests.public.l_quest_payment": 2989,
            "quests.public.h_client": 335,
            "quests.public.s_client_geo_info": 335,
            "quests.public.l_excursion_payment": 3001,
            "quests.public.s_client_personal_info": 3310,
            "quests.public.base": 88,
            "quests.public.calc": 132,
            "quests.public.big": 88,
            "quests.public.quarterly_sales": 12,
            "quests.public.sales_by_period": 12,
            "quests.public.s_excursion_geo_info": 12,
            "quests.public.l_author_quest": 44,
            "quests.public.s_payment_info": 388,
            "quests.public.raw_data": 44,
            "quests.public.dup": 44,
            "quests.public.h_author": 44,
            "quests.public.l_excursion_author": 44,
            "quests.public.h_category": 278,
            "quests.public.l_excursion_category": 278,
            "quests.public.l_quest_category": 278,
            "quests.public.h_episode": 559,
            "quests.public.s_episode_completion_info": 559,
            "quests.public.l_quest_episode": 559,
            "quests.public.h_quest": 559,
            "quests.public.l_payment_promo": 34
        },
        "column_usage_freq": {
            "quests.public.s_client_geo_info.region": 1116,
            "quests.public.l_payment_client.payment_dt": 1090,
            "quests.public.l_excursion_payment.payment_id": 3001,
            "quests.public.l_payment_client.payment_id": 5446,
            "quests.public.l_payment_client.client_id": 2970,
            "quests.public.h_client.id": 1140,
            "quests.public.s_client_geo_info.client_id": 570,
            "quests.public.l_quest_payment.payment_id": 2989,
            "quests.public.l_excursion_payment.excursion_id": 5242,
            "quests.public.l_quest_payment.quest_id": 5230,
            "quests.public.s_client_personal_info.client_id": 2728,
            "quests.public.s_client_personal_info.age": 14592,
            "quests.public.s_client_personal_info.registration_source": 1692,
            "quests.public.s_client_personal_info.first_purchase_date": 1604,
            "quests.public.s_client_personal_info.conversion_rate": 758,
            "quests.public.calc.registration_source": 88,
            "quests.public.calc.region": 88,
            "quests.public.calc.month": 88,
            "quests.public.calc.registered_users": 176,
            "quests.public.calc.buyers": 176,
            "quests.public.calc.avg_reg": 88,
            "quests.public.calc.max_buy": 88,
            "quests.public.calc.rn": 132,
            "quests.public.h_client.created_at": 176,
            "quests.public.big.registration_source": 264,
            "quests.public.big.region": 264,
            "quests.public.big.month": 176,
            "quests.public.big.registered_users": 176,
            "quests.public.big.buyers": 176,
            "quests.public.quarterly_sales.language": 12,
            "quests.public.quarterly_sales.city": 12,
            "quests.public.quarterly_sales.q1_sales": 48,
            "quests.public.quarterly_sales.q2_sales": 24,
            "quests.public.quarterly_sales.growth_rate": 12,
            "quests.public.s_excursion_geo_info.language": 24,
            "quests.public.s_client_geo_info.city": 24,
            "quests.public.s_excursion_geo_info.excursion_id": 12,
            "quests.public.sales_by_period.language": 24,
            "quests.public.sales_by_period.city": 24,
            "quests.public.sales_by_period.sales_count": 24,
            "quests.public.sales_by_period.quarter": 24,
            "quests.public.calc.author_id": 44,
            "quests.public.calc.author_name": 44,
            "quests.public.calc.total_sales": 44,
            "quests.public.calc.total_revenue": 44,
            "quests.public.calc.avg_exc": 44,
            "quests.public.calc.max_quest": 44,
            "quests.public.h_author.id": 132,
            "quests.public.h_author.name": 44,
            "quests.public.s_payment_info.amount": 778,
            "quests.public.l_excursion_author.author_id": 44,
            "quests.public.l_excursion_author.excursion_id": 44,
            "quests.public.s_payment_info.payment_id": 710,
            "quests.public.l_author_quest.author_id": 44,
            "quests.public.l_author_quest.quest_id": 44,
            "quests.public.raw_data.*": 44,
            "quests.public.dup.author_id": 220,
            "quests.public.dup.author_name": 88,
            "quests.public.dup.excursion_id": 88,
            "quests.public.dup.quest_id": 88,
            "quests.public.dup.exc_amount": 132,
            "quests.public.dup.quest_amount": 132,
            "quests.public.l_payment_client.purchase_count": 84,
            "quests.public.l_payment_client.client_count": 12,
            "quests.public.s_client_personal_info.loyalty_level": 64,
            "quests.public.l_payment_client.is_repeat_purchase": 64,
            "__unknown__.repeat_rate": 32,
            "quests.public.h_category.name": 1112,
            "quests.public.l_excursion_category.excursion_id": 278,
            "quests.public.l_excursion_category.category_id": 278,
            "quests.public.h_category.id": 556,
            "quests.public.l_quest_category.quest_id": 278,
            "quests.public.l_quest_category.category_id": 278,
            "quests.public.h_episode.name": 1118,
            "quests.public.h_quest.name": 1118,
            "quests.public.h_episode.id": 1677,
            "quests.public.s_episode_completion_info.time_spent": 559,
            "quests.public.s_episode_completion_info.episode_id": 559,
            "quests.public.l_quest_episode.episode_id": 559,
            "quests.public.l_quest_episode.quest_id": 559,
            "quests.public.h_quest.id": 559,
            "quests.public.s_episode_completion_info.client_id": 559,
            "__unknown__.completions": 559,
            "quests.public.l_payment_promo.promo_id": 102,
            "quests.public.l_payment_promo.payment_id": 34
        },
        "groupby_patterns": [
            {
            "queryid": "27a8890e-63d2-4078-a412-00ed39604ffc",
            "runquantity": 235,
            "columns_raw": [
                "scg.region",
                "DATE_TRUNC('MONTH', pc.payment_dt)"
            ],
            "columns_only": [
                "region"
            ]
            },
            {
            "queryid": "27a8890e-63d2-4078-a412-00ed39604ffc",
            "runquantity": 235,
            "columns_raw": [
                "scg.region",
                "DATE_TRUNC('MONTH', pc.payment_dt)"
            ],
            "columns_only": [
                "region"
            ]
            },
            {
            "queryid": "9af46d13-0ef4-4352-9a4c-1a400fcb8878",
            "runquantity": 2432,
            "columns_raw": [
                "CASE WHEN sci.age < 25 THEN '18-24' WHEN sci.age < 35 THEN '25-34' WHEN sci.age < 45 THEN '35-44' ELSE '45+' END"
            ],
            "columns_only": []
            },
            {
            "queryid": "dad15399-09ce-465e-b9e3-1712457e00c3",
            "runquantity": 758,
            "columns_raw": [
                "sci.registration_source"
            ],
            "columns_only": [
                "registration_source"
            ]
            },
            {
            "queryid": "15021609-4e0c-48fd-98e4-1e54bfe651cf",
            "runquantity": 88,
            "columns_raw": [
                "sci.registration_source",
                "scg.region",
                "DATE_TRUNC('MONTH', hc.created_at)"
            ],
            "columns_only": [
                "registration_source",
                "region"
            ]
            },
            {
            "queryid": "15021609-4e0c-48fd-98e4-1e54bfe651cf",
            "runquantity": 88,
            "columns_raw": [
                "sci.registration_source",
                "scg.region",
                "DATE_TRUNC('MONTH', hc.created_at)"
            ],
            "columns_only": [
                "registration_source",
                "region"
            ]
            },
            {
            "queryid": "28f3379d-9b67-4c1f-90f1-b8ee3e3fab52",
            "runquantity": 12,
            "columns_raw": [
                "segi.language",
                "scg.city",
                "DATE_TRUNC('QUARTER', pc.payment_dt)",
                "language",
                "city"
            ],
            "columns_only": [
                "language",
                "city",
                "language",
                "city"
            ]
            },
            {
            "queryid": "28f3379d-9b67-4c1f-90f1-b8ee3e3fab52",
            "runquantity": 12,
            "columns_raw": [
                "segi.language",
                "scg.city",
                "DATE_TRUNC('QUARTER', pc.payment_dt)"
            ],
            "columns_only": [
                "language",
                "city"
            ]
            },
            {
            "queryid": "28f3379d-9b67-4c1f-90f1-b8ee3e3fab52",
            "runquantity": 12,
            "columns_raw": [
                "language",
                "city"
            ],
            "columns_only": [
                "language",
                "city"
            ]
            },
            {
            "queryid": "3b1cc90f-d446-4592-becd-8c26efbabf56",
            "runquantity": 44,
            "columns_raw": [
                "author_id",
                "author_name",
                "excursion_id",
                "quest_id",
                "exc_amount",
                "quest_amount"
            ],
            "columns_only": [
                "author_id",
                "author_name",
                "excursion_id",
                "quest_id",
                "exc_amount",
                "quest_amount"
            ]
            },
            {
            "queryid": "3b1cc90f-d446-4592-becd-8c26efbabf56",
            "runquantity": 44,
            "columns_raw": [
                "author_id",
                "author_name",
                "excursion_id",
                "quest_id",
                "exc_amount",
                "quest_amount"
            ],
            "columns_only": [
                "author_id",
                "author_name",
                "excursion_id",
                "quest_id",
                "exc_amount",
                "quest_amount"
            ]
            },
            {
            "queryid": "dfcc8c65-1871-434d-a9de-fa31d0b7a59f",
            "runquantity": 12,
            "columns_raw": [
                "CASE WHEN purchase_count = 1 THEN 'Одноразовые' WHEN purchase_count BETWEEN 2 AND 5 THEN '2-5 покупок' WHEN purchase_count BETWEEN 6 AND 10 THEN '6-10 покупок' ELSE '10+ покупок' END",
                "client_id"
            ],
            "columns_only": [
                "client_id"
            ]
            },
            {
            "queryid": "dfcc8c65-1871-434d-a9de-fa31d0b7a59f",
            "runquantity": 12,
            "columns_raw": [
                "client_id"
            ],
            "columns_only": [
                "client_id"
            ]
            },
            {
            "queryid": "37a91112-1ccd-44a8-b150-a9cc4c76fc5f",
            "runquantity": 32,
            "columns_raw": [
                "sci.loyalty_level"
            ],
            "columns_only": [
                "loyalty_level"
            ]
            },
            {
            "queryid": "ee06b54f-5c53-4f4b-90ea-bf36ffe517cd",
            "runquantity": 278,
            "columns_raw": [
                "c.name"
            ],
            "columns_only": [
                "name"
            ]
            },
            {
            "queryid": "ee06b54f-5c53-4f4b-90ea-bf36ffe517cd",
            "runquantity": 278,
            "columns_raw": [
                "c.name"
            ],
            "columns_only": [
                "name"
            ]
            },
            {
            "queryid": "724dad7d-4263-4a03-a71e-34f977455713",
            "runquantity": 559,
            "columns_raw": [
                "he.id",
                "he.name",
                "hq.name"
            ],
            "columns_only": [
                "id",
                "name",
                "name"
            ]
            },
            {
            "queryid": "40122a54-5caa-4e1e-9866-90abfa9472d6",
            "runquantity": 34,
            "columns_raw": [
                "DATE_TRUNC('MONTH', pc.payment_dt)"
            ],
            "columns_only": []
            }
        ],
        "window_functions": {},
        "top_queries_by_q": [
            {
            "queryid": "9af46d13-0ef4-4352-9a4c-1a400fcb8878",
            "runquantity": 2432
            },
            {
            "queryid": "dad15399-09ce-465e-b9e3-1712457e00c3",
            "runquantity": 758
            },
            {
            "queryid": "724dad7d-4263-4a03-a71e-34f977455713",
            "runquantity": 559
            },
            {
            "queryid": "ee06b54f-5c53-4f4b-90ea-bf36ffe517cd",
            "runquantity": 278
            },
            {
            "queryid": "27a8890e-63d2-4078-a412-00ed39604ffc",
            "runquantity": 235
            },
            {
            "queryid": "15021609-4e0c-48fd-98e4-1e54bfe651cf",
            "runquantity": 88
            },
            {
            "queryid": "3b1cc90f-d446-4592-becd-8c26efbabf56",
            "runquantity": 44
            },
            {
            "queryid": "40122a54-5caa-4e1e-9866-90abfa9472d6",
            "runquantity": 34
            },
            {
            "queryid": "37a91112-1ccd-44a8-b150-a9cc4c76fc5f",
            "runquantity": 32
            },
            {
            "queryid": "28f3379d-9b67-4c1f-90f1-b8ee3e3fab52",
            "runquantity": 12
            }
        ],
        "hot_join_cliques": [
            [
            "quests.public.h_episode",
            "quests.public.s_episode_completion_info"
            ],
            [
            "quests.public.h_episode",
            "quests.public.l_quest_episode"
            ],
            [
            "quests.public.l_payment_client",
            "quests.public.s_client_personal_info"
            ]
        ],
        "hot_columns_per_table": {
            "quests.public.s_client_geo_info": [
            "region",
            "client_id",
            "city"
            ],
            "quests.public.l_payment_client": [
            "payment_id",
            "client_id",
            "payment_dt",
            "purchase_count",
            "is_repeat_purchase",
            "client_count"
            ],
            "quests.public.l_excursion_payment": [
            "excursion_id",
            "payment_id"
            ],
            "quests.public.h_client": [
            "id",
            "created_at"
            ],
            "quests.public.l_quest_payment": [
            "quest_id",
            "payment_id"
            ],
            "quests.public.s_client_personal_info": [
            "age",
            "client_id",
            "registration_source",
            "first_purchase_date",
            "conversion_rate",
            "loyalty_level"
            ],
            "quests.public.calc": [
            "registered_users",
            "buyers",
            "rn",
            "registration_source",
            "region",
            "month",
            "avg_reg",
            "max_buy",
            "author_id",
            "author_name"
            ],
            "quests.public.big": [
            "registration_source",
            "region",
            "month",
            "registered_users",
            "buyers"
            ],
            "quests.public.quarterly_sales": [
            "q1_sales",
            "q2_sales",
            "language",
            "city",
            "growth_rate"
            ],
            "quests.public.s_excursion_geo_info": [
            "language",
            "excursion_id"
            ],
            "quests.public.sales_by_period": [
            "language",
            "city",
            "sales_count",
            "quarter"
            ],
            "quests.public.h_author": [
            "id",
            "name"
            ],
            "quests.public.s_payment_info": [
            "amount",
            "payment_id"
            ],
            "quests.public.l_excursion_author": [
            "author_id",
            "excursion_id"
            ],
            "quests.public.l_author_quest": [
            "author_id",
            "quest_id"
            ],
            "quests.public.raw_data": [
            "*"
            ],
            "quests.public.dup": [
            "author_id",
            "exc_amount",
            "quest_amount",
            "author_name",
            "excursion_id",
            "quest_id"
            ],
            "quests.public.h_category": [
            "name",
            "id"
            ],
            "quests.public.l_excursion_category": [
            "excursion_id",
            "category_id"
            ],
            "quests.public.l_quest_category": [
            "quest_id",
            "category_id"
            ],
            "quests.public.h_episode": [
            "id",
            "name"
            ],
            "quests.public.h_quest": [
            "name",
            "id"
            ],
            "quests.public.s_episode_completion_info": [
            "time_spent",
            "episode_id",
            "client_id"
            ],
            "quests.public.l_quest_episode": [
            "episode_id",
            "quest_id"
            ],
            "quests.public.l_payment_promo": [
            "promo_id",
            "payment_id"
            ]
        }
        }

    for i in range(1, MOCK_TASK_PROCESSING_TICS + 1):
        logger.info(f"sleeping for {SLEEP_INTERVAL_SECS * i} seconds")
        sleep(SLEEP_INTERVAL_SECS)

    return mock_response_for_do_work()


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
