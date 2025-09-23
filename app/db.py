from typing import Generator

from fastapi import Depends, Request
from sqlalchemy.orm import Session, sessionmaker

from app.schema import Base
from sqlalchemy import (
    create_engine,
)
from sqlalchemy.engine import Engine


def create_engine_from_url(url: str) -> Engine:
    engine = create_engine(
        url, pool_pre_ping=True, connect_args={"check_same_thread": False}
    )
    if engine.dialect.name == "sqlite":
        with engine.connect() as conn:
            conn.exec_driver_sql("PRAGMA journal_mode=WAL;")
            conn.exec_driver_sql("PRAGMA foreign_keys=ON;")
    return engine


def create_tables(engine: Engine) -> None:
    Base.metadata.create_all(bind=engine)


def get_sessionmaker(request: Request) -> sessionmaker:
    """
    Return the app’s configured sessionmaker.
    :param request: fastapi Request object
    :return: sqlalchemy sessionmaker object
    """
    return request.app.state.SessionLocal


def get_session(
    SessionLocal: sessionmaker = Depends(get_sessionmaker),
) -> Generator[Session, None, None]:
    """
    Create a generator that provides a database session. The session is automatically closed after use,
    and any exceptions during the session are handled by rolling back the transaction.
    :return: A generator that yields a database session.
    """
    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
