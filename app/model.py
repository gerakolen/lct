from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from uuid import UUID


# Model should be moved to routers
class DDLStatement(BaseModel):
    statement: str = Field(..., description="SQL CREATE TABLE statement")


class QueryItem(BaseModel):
    queryid: UUID = Field(..., description="Unique query ID")
    query: str = Field(..., description="SQL query")
    runquantity: int = Field(..., ge=0, description="Number of times the query is run")


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

# TODO confirm if we need it
# class ResultResponse(BaseModel):
#     ddl: List[DDLStatement]
#     migrations: List[DDLStatement]  # Reusing DDLStatement for migration statements
#     queries: List[QueryItem]


class TaskResultResponse(BaseModel):
    taskid: str
    status: str
    result: Optional[Dict[str, Any]] = None
