
from sqlalchemy import Column, DateTime, Enum, Text
from sqlalchemy.types import String


import enum
import uuid
from sqlalchemy.dialects.sqlite import JSON as SQLITE_JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class Base(DeclarativeBase):
    pass


class Task(Base):
    __tablename__ = "tasks"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(Enum(TaskStatus), nullable=False, default=TaskStatus.PENDING)
    result = Column(SQLITE_JSON, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, default=func.now(), onupdate=func.now(), nullable=False
    )
