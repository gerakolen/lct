from contextlib import closing
from trino.dbapi import connect
from trino.auth import BasicAuthentication

from app.config import LCTSettings


def get_trino_connection(settings: LCTSettings):
    t = settings.trino
    return connect(
        host=t.host,
        port=t.port,
        user=t.username,
        auth=BasicAuthentication(t.username, t.password),
    )


def explain_analyze(sql: str, settings: LCTSettings) -> str:
    q = sql.strip().rstrip(";")
    if not q.upper().startswith("EXPLAIN"):
        q = f"EXPLAIN ANALYZE {q}"
    with closing(get_trino_connection(settings)) as conn, conn.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()

        return "\n".join(str(r[0]) for r in rows)
