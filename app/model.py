from typing import List
from pydantic import BaseModel, Field
from uuid import UUID


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


class ResultResponse(BaseModel):
    ddl: List[DDLStatement]
    migrations: List[DDLStatement]  # Reusing DDLStatement for migration statements
    queries: List[QueryItem]
