import uuid
from typing import List
from pydantic import BaseModel, Field
from uuid import UUID


# Model should be moved to routers
class DDLStatement(BaseModel):
    statement: str = Field(..., description="SQL CREATE TABLE statement")


class QueryItem(BaseModel):
    queryid: UUID = Field(..., description="Unique query ID")
    query: str = Field(..., description="SQL query")
    runquantity: int = Field(..., ge=0, description="Number of times the query is run")
    executiontime: int = Field(..., ge=0, description="Execution Time")


class NewTaskRequest(BaseModel):
    url: str = Field(..., description="JDBC connection string")
    ddl: List[DDLStatement] = Field(..., description="List of DDL statements")
    queries: List[QueryItem] = Field(..., description="List of queries with stats")


class TaskResponse(BaseModel):
    taskid: UUID


class StatusResponse(BaseModel):
    status: str  # RUNNING, DONE, FAILED


class ExplainRequest(BaseModel):
    sql: str


class ExplainResponse(BaseModel):
    plan: str


class TaskResultResponse(BaseModel):
    ddl: List[DDLStatement]
    migrations: List[DDLStatement]
    queries: List[QueryItem]


# TODO used only in tests, should be removed
def create_mock_response() -> TaskResultResponse:
    return TaskResultResponse(
        ddl=[
            DDLStatement(
                statement="CREATE TABLE quests.public.h_author ( id integer, name varchar, created_at timestamp(6));"
            )
        ],
        migrations=[
            DDLStatement(
                statement="INSERT INTO quests.public.h_author_new SELECT * FROM quests.public.h_author;"
            )
        ],
        queries=[
            QueryItem(
                queryid=uuid.UUID("3b1cc90f-d446-4592-becd-8c26efbabf56"),
                runquantity=44,
                executiontime=25,
                query="WITH raw_data AS (SELECT ha.id AS author_id, ha.name AS author_name, ep.excursion_id, qp.quest_id, ei.amount AS exc_amount, qi.amount AS quest_amount FROM quests.public.h_author ha LEFT JOIN quests.public.l_excursion_author ea ON ha.id = ea.author_id LEFT JOIN quests.public.l_excursion_payment ep ON ea.excursion_id = ep.excursion_id LEFT JOIN quests.public.s_payment_info ei ON ep.payment_id = ei.payment_id LEFT JOIN quests.public.l_author_quest aq ON ha.id = aq.author_id LEFT JOIN quests.public.l_quest_payment qp ON aq.quest_id = qp.quest_id LEFT JOIN quests.public.s_payment_info qi ON qp.payment_id = qi.payment_id LIMIT 500000), dup AS (SELECT r.*, x.n AS mult FROM raw_data r CROSS JOIN (VALUES 1,2) AS x(n)), calc AS (SELECT author_id, author_name, COUNT(excursion_id) + COUNT(quest_id) AS total_sales, COALESCE(SUM(exc_amount),0) + COALESCE(SUM(quest_amount),0) AS total_revenue, ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY random()) AS rn, AVG(exc_amount) OVER (PARTITION BY author_id) AS avg_exc, MAX(quest_amount) OVER (PARTITION BY author_id) AS max_quest FROM dup GROUP BY author_id, author_name, excursion_id, quest_id, exc_amount, quest_amount) SELECT author_id, author_name, total_sales, total_revenue, avg_exc, max_quest FROM calc WHERE rn < 500 ORDER BY random();",
            )
        ],
    )
