from contextlib import closing

from pydantic_core._pydantic_core import ValidationError
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from urllib.parse import urlparse, parse_qs

from app.config import LCTSettings, TrinoSettings


def get_trino_connection(settings: LCTSettings):
    t = settings.trino
    return connect(
        host=t.host,
        port=t.port,
        user=t.username,
        auth=BasicAuthentication(t.username, t.password),
    )


def extract_connection_details(url: str) -> TrinoSettings:
    if url.startswith("jdbc:"):
        url = url[5:]
    parsed = urlparse(url)

    host = parsed.hostname or "localhost"
    port = parsed.port or 443

    query_params = parse_qs(parsed.query)
    user = query_params.get("user", [None])[0]
    password = query_params.get("password", [None])[0]

    if not user or not password:
        raise ValidationError.from_exception_data(
            "User and password must be provided in the URL.", line_errors=[]
        )

    return TrinoSettings(host=host, port=port, username=user, password=password)


def explain_analyze(sql: str, settings: LCTSettings) -> str:
    q = sql.strip().rstrip(";")
    if not q.upper().startswith("EXPLAIN"):
        q = f"EXPLAIN ANALYZE {q}"
    with closing(get_trino_connection(settings)) as conn, conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()

        return "\n".join(str(r[0]) for r in rows)
