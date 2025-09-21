from sqlalchemy import Column, String, create_engine

from sqlalchemy.types import TypeDecorator, CHAR
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
import uuid

Base = declarative_base()

# https://gist.github.com/gmolveau/7caeeefe637679005a7bb9ae1b5e421e
class GUID(TypeDecorator):
    """Platform-independent GUID type.
    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.
    """
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value).int
            else:
                # hexstring
                return "%.32x" % value.int

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value


class Task(Base):
    __tablename__ = "tasks"    
    id = Column(GUID(), primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(String, default="RUNNING")
    result = Column(String, nullable=True)  # JSON string for result


# Create DB (SQLite for simplicity)
engine = create_engine("sqlite:///tasks.db")
Base.metadata.create_all(engine)
