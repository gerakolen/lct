import logging
from contextlib import asynccontextmanager
from typing import Dict, AsyncIterator
from sqlalchemy.orm import sessionmaker

from fastapi import FastAPI

from app.config import LCTSettings, lct_settings
from app.routers import task
from app.db import create_engine_from_url, create_tables

logger = logging.getLogger(__name__)


def create_app(settings: LCTSettings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine_from_url(settings.db.url)
        create_tables(engine)

        app.state.engine = engine
        app.state.settings = settings
        app.state.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=engine
        )

        yield

        app.state.engine.dispose()

    app = FastAPI(lifespan=lifespan)
    app.include_router(task.router)
    return app


app = create_app(lct_settings)


@app.get("/")
def read_root() -> Dict[str, str]:
    return {"Hello": "World"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8998)
