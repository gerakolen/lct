import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator
from sqlalchemy.orm import sessionmaker

from fastapi import FastAPI, Depends
from fastapi.responses import HTMLResponse

from app.config import LCTSettings, lct_settings
from app.routers import task
from app.db import create_engine_from_url, create_tables
from app.security import require_basic_auth


from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

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
    # app.include_router(task.router)
    app.include_router(task.router, dependencies=[Depends(require_basic_auth)])
    return app


app = create_app(lct_settings)


@app.get("/", response_class=HTMLResponse)
def read_root():
    html_content = """
    <html>
        <head>
            <title>LCT App Info</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 40px; }
                pre { background: #f4f4f4; padding: 10px; border-radius: 5px; }
            </style>
        </head>
        <body>
            <h2>Team: fortuna</h2>
            <p>LCT app version 1.0.0</p>
            <h3>Available endpoints:</h3>
            <h4>/docs - Swagger UI</h4>
            <h4>/new - launch a task</h4>
            <pre>
curl -u "$USERNAME:$PASSWORD" \\
    -X POST http://lct-host:8998/new \\
    -H "Content-Type: application/json" \\
    -d '{
      "url": "jdbc:trino://trino.czxqx2r9.data.bizmrg.com:443?user=admint&password=secret",
      "ddl": [
        {"statement": "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))"}
      ],
      "queries": [
        {
          "queryid": "0197a0b2-2284-7af8-9012-fcb21e1a9785",
          "query": "SELECT u.id, u.name, COUNT(o.order_id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id",
          "runquantity": 123,
          "executiontime": 12
        }
      ]
    }'
            </pre>
            <h4>/status - check the status of a task</h4>
            <pre>
curl -s -u $(USERNAME):$(PASSWORD) http://lct-host:8998/status?task_id=$(TASK_ID)
            </pre>
            <h4>/getresult - get results of a task</h4>
            <pre>
curl -u $(USERNAME):$(PASSWORD)  http://lct-host:8998/getresult?task_id=$(TASK_ID) | jq .
            </pre>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(_request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content={"detail": exc.errors(), "body": exc.body},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8998)
