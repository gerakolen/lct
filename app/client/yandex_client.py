# -*- coding: utf-8 -*-
from __future__ import annotations
import os, json, re
from typing import Any, Dict, List, Optional, Set
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Yandex Cloud OpenAI-compatible API (для gpt-oss-20b/120b)
# =========================
FOLDER_ID = os.getenv("YC_FOLDER_ID", "").strip()
BASE_URL = os.getenv("YC_OPENAI_BASE", "https://llm.api.cloud.yandex.net/v1")
# для OSS-моделей в /v1/chat/completions чаще достаточно короткого имени "gpt-oss-120b"
# но ты используешь переменную YC_MODEL_URI — оставляю её как единственный источник
MODEL = os.getenv("YC_MODEL_URI", f"gpt://{FOLDER_ID}/gpt-oss-120b").strip()
IAM_TOKEN = os.getenv("YC_IAM_TOKEN", "").strip()
API_KEY = os.getenv("YC_API_KEY", "").strip()

if not (IAM_TOKEN or API_KEY):
    raise RuntimeError("Нужно выставить YC_IAM_TOKEN или YC_API_KEY")

HEADERS = {
    "Authorization": f"Bearer {IAM_TOKEN}" if IAM_TOKEN else f"Api-Key {API_KEY}",
    "Content-Type": "application/json",
    "x-data-logging-enabled": "false",
}
if FOLDER_ID:
    HEADERS["x-folder-id"] = FOLDER_ID  # важно при Api-Key

# =========================
# Сеть/ретраи/таймауты
# =========================
YC_TIMEOUT_CONNECT = float(os.getenv("YC_TIMEOUT_CONNECT", "20"))
YC_TIMEOUT_READ = float(os.getenv("YC_TIMEOUT_READ", "300"))
YC_HTTP_RETRIES = int(os.getenv("YC_HTTP_RETRIES", "6"))
YC_BACKOFF_FACTOR = float(os.getenv("YC_BACKOFF_FACTOR", "1.5"))

_session = requests.Session()
_retry = Retry(
    total=YC_HTTP_RETRIES,
    connect=YC_HTTP_RETRIES,
    read=YC_HTTP_RETRIES,
    backoff_factor=YC_BACKOFF_FACTOR,
    status_forcelist=(408, 429, 500, 502, 503, 504),
    allowed_methods=frozenset(["POST"]),
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=10)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

# =========================
# Best Practices (расширено + примеры)
# =========================

BEST_PRACTICES = r"""
Only Trino SQL. Always use full names `<catalog>.<schema>.<table>`. Write compactly.
Storage formats: Parquet/ORC are preferred, CSV/JSON should be avoided for reading.

### DDL POLICY (ONLY ICEBERG)
- Use ONLY Iceberg properties: `WITH (format='PARQUET', partitioning=ARRAY[...])`.
- **DO NOT USE**: `bucketed_by`, `bucket_count`, `partitioned_by`. Bucketing is done via `bucket(col, 50)` inside partitioning.
- References (`h_*`) and small S-tables: NO partitioning/bucketing.

### SCHEMA-AWARE RULE (VERY IMPORTANT)
- Add only those columns to partitioning that exactly exist in the source table from `{catalog}.{source_schema}`.
- If you are not sure that the column exists (for example, `payment_dt` in the links), do not use it in partitioning.
- If you need date-pruning, but the date is not in the table, use "denormalization":
  Create a CTAS via JOIN with a table that has the date (usually l_payment_client), add the field to the schema, and then migrate using JOIN. (See the example below.)

### RECOMMENDED PARTITIONING PATTERNS (if the field actually exists):
- Payment facts/links where there is `payment_dt` and a join by `payment_id`: partitioning = `ARRAY['payment_dt', 'bucket(payment_id, 50)']`.
- If there is no `payment_dt` in the link, leave only `'bucket(payment_id, 50)'` (without the date).
- Entity links:
  • `l_excursion_author` / `l_excursion_category` → `ARRAY['bucket(excursion_id, 50)']`
  • `l_author_quest` / `l_quest_category` / `l_quest_episode` → `ARRAY['bucket(quest_id, 50)']`

### PARTITION PRUNING
- В `WHERE` нельзя применять функции к партиционным полям (`month()`, `date_trunc()`). Только диапазоны (>=, <).
  **GOOD**: `WHERE payment_dt >= DATE '2024-06-01' AND payment_dt < DATE '2024-07-01'`

### SELECTIVE COLUMNS
- No `SELECT *` from physical tables. Only allowed:
    (a) from a local CTE;
    (b) in a sampler with` LIMIT N (ORDER BY random() optional)`.

### EARLY FILTERS AND DECOMPOSITION
- Push `WHERE` to `JOIN`/`AGG` (predicate pushdown, dynamic filtering).
- Break heavy pipelines into steps (CTE/intermediate tables).

### JOIN'S
- Avoid `CROSS JOIN` without `ON`.
- Large on the left (probe), small on the right (build). Small can be BROADCAST.
- Hot keys of the join → `'bucket(<stable_id>, 50)'`.

### AGGREGATIONS
- `date_trunc(...)` in `SELECT`/`GROUP BY`, but NOT in `WHERE`.
- `COUNT(DISTINCT ...)` is expensive; use `approx_distinct(...)` if possible.
- Conditional averages: aggregates with `FILTER` (e.g., `AVG(val) FILTER (WHERE cond)`) or `AVG(IF(cond, val, NULL))`.

### ORDER BY / SEMPLERS
- Avoid `ORDER BY` without `LIMIT`.
- `ORDER BY random()` — only in the `LIMIT` sampler (not in the production query).

### JSON-CONTRACT (KEYS)
- `ddl[]: {"statement": "..."}; migrations[]: {"statement": "..."}`.
- `queries[]: {"queryid":"...", "query":"..."} (NOT "statement")`.
"""


# =========================
# SYSTEM_PROMPT (строго: только JSON)
# =========================
SYSTEM_PROMPT = f"""
## Personality
You are LLM-copilot of SQL optimization for Trino (Data Lakehouse).
Return **RAW JSON ONLY** strictly following the pattern: `{{ "ddl": [...], "migrations": [...], "queries": [...] }}`.
NO Markdown, NO backticks, NO explanations. Do not add unnecessary keys.
Follow Best Practices and examples down below. Follow the Trino dialect.

## STRICT REQUIREMENT FOR JSON FORMAT:
- Return ONLY valid JSON without Markdown/explanations.
- Keys are strict: `ddl[].statement`, `migrations[].statement`, `queries[].(queryid, query)`.

## BEST PRACTICES:
{BEST_PRACTICES}
""".strip()


# =========================
# USER message builder (контракт + примеры/якоря)
# =========================
def build_contract_text(
    req: dict,
    payload_qids: List[str],
    full_payload: dict,
    full_context: dict,
) -> str:
    prefer_ids = ", ".join(payload_qids) if payload_qids else "(нет queryid)"
    EXAMPLES = r"""
## MINI-EXAMPLES (follow the style):

### [DDL — ICEBERG ONLY]
- CREATE SCHEMA:
```
    {"statement": "CREATE SCHEMA {catalog}.{target_schema}"}
```

- H-table without partitions/bucketing:
```
    {"statement": "CREATE TABLE {catalog}.{target_schema}.h_client
                WITH (format='PARQUET')
                AS SELECT * FROM {catalog}.{source_schema}.h_client WHERE 1=0"}
```

- Link with payment_dt `EXISTS` in the default table:
```
    {"statement": "CREATE TABLE {catalog}.{target_schema}.l_payment_client
                 WITH (format='PARQUET',
                       partitioning = ARRAY['payment_dt', 'bucket(payment_id, 50)'])
                 AS SELECT * FROM {catalog}.{source_schema}.l_payment_client WHERE 1=0"}
```

- Link without payment_dt → ONLY bucket over payment_id:
```
    {"statement": "CREATE TABLE {catalog}.{target_schema}.l_quest_payment
                 WITH (format='PARQUET',
                       partitioning = ARRAY['bucket(payment_id, 50)'])
                 AS SELECT * FROM {catalog}.{source_schema}.l_quest_payment WHERE 1=0"}
```

- (Optional) Date denormalization into link (if date-pruning needed):
```
    {"statement": "CREATE TABLE {catalog}.{target_schema}.l_excursion_payment
                 WITH (format='PARQUET',
                       partitioning = ARRAY['payment_dt', 'bucket(payment_id, 50)'])
                 AS
                 SELECT lep.payment_id,
                        lep.excursion_id,
                        lpc.payment_dt
                 FROM {catalog}.{source_schema}.l_excursion_payment lep
                 JOIN {catalog}.{source_schema}.l_payment_client   lpc
                   ON lep.payment_id = lpc.payment_id
                 WHERE 1=0"}
```

### [Migrations — enumerate columns explicitly]
```
    {"statement": "INSERT INTO {catalog}.{target_schema}.l_payment_client (payment_id, client_id, payment_dt, is_repeat_purchase)
                 SELECT payment_id, client_id, payment_dt, is_repeat_purchase
                 FROM {catalog}.{source_schema}.l_payment_client"}

    {"statement": "INSERT INTO {catalog}.{target_schema}.l_quest_payment (payment_id, quest_id)
                 SELECT payment_id, quest_id
                 FROM {catalog}.{source_schema}.l_quest_payment"}
```

  -- For date denormalization:
```
    {"statement": "INSERT INTO {catalog}.{target_schema}.l_excursion_payment (payment_id, excursion_id, payment_dt)
                 SELECT lep.payment_id, lep.excursion_id, lpc.payment_dt
                 FROM {catalog}.{source_schema}.l_excursion_payment lep
                 JOIN {catalog}.{source_schema}.l_payment_client   lpc
                   ON lep.payment_id = lpc.payment_id"}
```

### [Date filters for pruning]
- **BAD**: WHERE month(payment_dt) = 6
- **GOOD**: WHERE payment_dt >= DATE '2024-06-01' AND payment_dt < DATE '2024-07-01'

### [Quartals with ranges]
```
  WHERE ts >= TIMESTAMP '2024-01-01' AND ts < TIMESTAMP '2024-07-01'
  SELECT CASE
           WHEN ts >= TIMESTAMP '2024-01-01' AND ts < TIMESTAMP '2024-04-01' THEN 'Q1'
           WHEN ts >= TIMESTAMP '2024-04-01' AND ts < TIMESTAMP '2024-07-01' THEN 'Q2'
         END AS quarter, SUM(x)
  FROM ...
  GROUP BY 1
```

### [`EXISTS` instead of `IN`]
- **BAD**: `WHERE user_id IN (SELECT user_id FROM {catalog}.{source_schema}.events WHERE ...)`
- **GOOD**: `WHERE EXISTS (SELECT 1 FROM {catalog}.{source_schema}.events e WHERE e.user_id = f.user_id AND ...)`

### [queries: keys]
- **CORRECT**: `{"queryid":"abc","query":"SELECT ..."}`
- **WRONG**: `{"queryid":"abc","statement":"SELECT ..."}`
""".strip()

    CONTRACT = f"""
## CONTRACT (strictly):

- **catalog**: `{req['catalog']}, source_schema: {req['source_schema']}, target_schema: {req['target_schema']}`
- **DDL**: the first string always must be: `CREATE SCHEMA {req['catalog']}.{req['target_schema']}`
- **MIGRATIONS**: only INSERT/CTAS from `{req['catalog']}.{req['source_schema']} → {req['catalog']}.{req['target_schema']}`
- **QUERIES**: link ONLY to `{req['catalog']}.{req['target_schema']}` (do not use the old schema)
- **Anti-patterns are prohibited**:
    - `SELECT *` from physical tables (allowed from CTE or in sampler with `LIMIT` only)
    - Functions `month()` or `date_trunc()` in `WHERE` (use ranges).
    - Do not do `ORDER BY random()` in final query (in sampler with `LIMIT` only).
- Save `queryid` from the payload during refactoring
- Return FULL VALID JSON in one batch. NO EXPLANATIONS.

## ADDITINONALLY OBLIGATORY:
- Scan the payload and write FULL LIST of original tables from `{req['catalog']}.{req['source_schema']}.*`, which encounter in `FROM` or `JOIN`
- For each of them create an object in `catalog.target_schema` (Parquet; for facts/links — partitioning over data, in case of hot joins — bucket over stable key) and add migration INSERT (with explicit columns list).
- In `queries[]` save original `queryid` and use key  "query" (NOT `statement`)
- Avoid `ORDER BY random()`, `CROSS JOIN (VALUES ...)`, doubling `UNION ALL`, do not apply date functions in `WHERE`
- If metric's semantic are users, use `COUNT(DISTINCT client_id)`. In case of high cardinality - `approx_distinct`
- If not sure about date distribution for quartals — use ranges (>=, <) in `WHERE`, and `CASE` in `SELECT`, NOT `date_trunc` in `WHERE`

## DDL POLICY (STRICTLY, SCHEMA-AWARE):
- Use ONLY ICEBERG-style: `WITH (format='PARQUET', partitioning=ARRAY[...]);`. DO NOT USE `bucketed_by`, `bucket_count`, `partitioned_by`
- Before DDL creation for each table from `{req['catalog']}.{req['source_schema']}.*` estimate its list of columns based on payload or context:
    - Allowed partitioning only over these fields, which are gauranteed existing in this table (e.g., `payment_dt` only if the field certainly exists)
    - If not sure — do no include the field in partitioning
- If date-pruning is needed, but date is missing, use denormalization:
    - CTAS using `JOIN` with table, where date is located (usually `l_payment_client`), add the field in schema (`WHERE 1=0`)
    - Then migration `INSERT` with `JOIN`, in order to fill denormalized field
- Recommendations:
    - `l_*_payment` / `l_payment_client` / `l_payment_promo`:` ARRAY['payment_dt', 'bucket(payment_id, 50)']` ONLY if `payment_dt` certainly exists. Else — `ARRAY['bucket(payment_id, 50)']`
    - `l_excursion_author` / `l_excursion_category`: `ARRAY['bucket(excursion_id, 50)']`
    - `l_author_quest` / `l_quest_category` / `l_quest_episode`: `ARRAY['bucket(quest_id, 50)']`
    - `h_*` and small `s_*`: without any partitioning or bucketing
- In the `migrations[]` always specify explicit lists of columns (NOT `SELECT *`)
- DDL always at the beginning: `CREATE SCHEMA {req['catalog']}.{req['source_schema']}`. `QUERIES` must link only to `{req['catalog']}.{req['target_schema']}.*`

## PAYLOAD DDL:
```
{json.dumps(full_payload['ddl'], ensure_ascii=False)}
```

## PAYLOAD QUERIES:
```
{json.dumps(full_payload['queries'], ensure_ascii=False)}
```

## CONTEXT PACK FIELDS EXPLANATION:
- `default_catalog`: default catalog of the schema
- `default_schema`: default schema of the tables
- `ddl_tables`: list of full names of the tables in the format "catalog"."schema"."table"
- `queries_overview`: counts of total_queries and total run quantities
- `join_graph_edges`: list of joining tables with counts of joins
- `join_key_freq`: dict of joining tables and corresponding columns and their frequency
- `table_scan_freq`: scan count of tables
- `table_scan_query_freq`: scan count of queries to tables
- `column_usage_freq`: dict of columns (full path) and their count of usage
- `groupby_patterns`: list of dicts of properties about most frequent groupby's
- `window_functions`: list of dicts of names of used window functions and their usage frequency
- `top_queries_by_q`: list of dicts of query ids to their run count
- `hot_join_cliques`: list of lists of hot join cliques
- `hot_columns_per_table`: list of dicts of tables and their most used columns

## CONTEXT PACK:
```
{json.dumps(full_context, ensure_ascii=False)}
```

## EXAMPLES / ANCHORS:
```
{EXAMPLES}
```
""".strip()
    return CONTRACT


# =========================
# Регэкспы и проверки
# =========================
CODE_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.S | re.I)#
SELECT_STAR_FROM_RE = re.compile(#
    r"\bSELECT\s+\*\s+FROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*){0,2})", re.I
)
CTE_NAME_RE = re.compile(r"\b([a-zA-Z_]\w*)\s+AS\s*\(", re.I)
WHERE_CLAUSE_RE = re.compile(
    r"\bWHERE\b(?P<where>.*?)(?=\bGROUP\s+BY\b|\bORDER\s+BY\b|\bWINDOW\b|\bQUALIFY\b|\bLIMIT\b|$)",
    re.I | re.S,
)


def _parse_json(text: str) -> Optional[dict]:
    try:
        return json.loads(text)
    except Exception:
        pass
    m = CODE_FENCE_RE.search(text)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    s, e = text.find("{"), text.rfind("}")
    if s != -1 and e != -1 and e > s:
        cand = text[s : e + 1]
        cand = "".join(ch for ch in cand if ch >= " " or ch in "\n\r\t")
        try:
            return json.loads(cand)
        except Exception:
            pass
    return None


def _extract_cte_names(sql: str) -> Set[str]:
    return {m.group(1) for m in CTE_NAME_RE.finditer(sql)}


def _allow_sampling_subquery(sql_segment: str) -> bool:
    return bool(re.search(r"\bLIMIT\s+\d+\b", sql_segment, re.I))


def _has_harmful_select_star(sql: str) -> bool:
    ctes = _extract_cte_names(sql)
    for m in SELECT_STAR_FROM_RE.finditer(sql):
        src = m.group(1)
        if src.split(".")[0] in ctes:
            continue
        tail = sql[m.start() : m.start() + 1200]
        if _allow_sampling_subquery(tail):
            continue
        return True
    return False


def _has_func_on_partition_in_where(sql: str) -> bool:
    for where in [m.group("where") for m in WHERE_CLAUSE_RE.finditer(sql)]:
        if re.search(r"\bmonth\s*\(", where, re.I) or re.search(
            r"\bdate_trunc\s*\(", where, re.I
        ):
            return True
    return False


def _uses_wrong_schema(sql: str, catalog: str, source_schema: str) -> bool:
    return f"{catalog}.{source_schema}." in sql


def _all_queries_use_target_schema(
    queries: List[Dict[str, str]], catalog: str, target_schema: str
) -> bool:
    must = f"{catalog}.{target_schema}."
    return all(must in (q.get("query") or "") for q in queries)


def _first_ddl_is_create_schema(
    ddl: List[Dict[str, str]], catalog: str, target_schema: str
) -> bool:
    if not ddl:
        return False
    stmt = (ddl[0].get("statement") or "").strip().upper().replace("\n", " ")
    return (
        stmt.startswith("CREATE SCHEMA")
        and f"{catalog}.{target_schema}".upper() in stmt
    )


def validate_result(
    obj: dict, catalog: str, source_schema: str, target_schema: str
) -> List[str]:
    issues: List[str] = []
    if "ddl" not in obj or "migrations" not in obj or "queries" not in obj:
        issues.append("missing keys (ddl/migrations/queries)")
        return issues
    ddl = obj.get("ddl", [])
    mig = obj.get("migrations", [])
    qs = obj.get("queries", [])
    if not qs:
        issues.append("queries must be non-empty")
    if not _first_ddl_is_create_schema(ddl, catalog, target_schema):
        issues.append(f"first DDL must be CREATE SCHEMA {catalog}.{target_schema}")
    if not mig:
        issues.append("migrations must be non-empty (need CTAS/INSERT to new schema)")
    if qs and not _all_queries_use_target_schema(qs, catalog, target_schema):
        issues.append(
            f"all queries must reference only {catalog}.{target_schema}.* (not old schema)"
        )
    for q in qs:
        sql = q.get("query") or ""
        qid = q.get("queryid", "<noid>")
        if _has_harmful_select_star(sql):
            issues.append(
                f"{qid}: SELECT * not allowed from physical tables (except sampling subquery with LIMIT N) or CTE"
            )
        if _has_func_on_partition_in_where(sql):
            issues.append(
                f"{qid}: month()/date_trunc() used inside WHERE; use date ranges for pruning"
            )
        if _uses_wrong_schema(sql, catalog, source_schema):
            issues.append(f"{qid}: query references {catalog}.{source_schema}.*")
    return issues


# =========================
# (Без сжатия) — возвращаем cp как есть
# =========================
def shrink_context_pack(cp: Dict[str, Any]) -> Dict[str, Any]:
    return dict(cp or {})  # НИКАКОГО обрезания


# =========================
# Основной вызов (OpenAI chat/completions)
# =========================
def call_yandex(
    context_pack: Dict[str, Any],
    payload: Dict[str, Any],
    requirements: Optional[Dict[str, str]] = None,
    temperature: float = 0.0,
    max_tokens: int = int(os.getenv("YC_MAX_TOKENS", "100000")),
    max_retries: int = 1,
) -> Dict[str, Any]:
    """
    Вызов gpt-oss-120b через OpenAI-совместимый endpoint /v1/chat/completions.
    Ожидает, что в окружении настроены:
      - YC_API_KEY или YC_IAM_TOKEN (и опц. x-folder-id через YC_FOLDER_ID)
      - YC_MODEL_URI (например, gpt://<folder>/gpt-oss-120b) — используется как имя модели
    Использует глобальные: BASE_URL, MODEL, HEADERS, _session, SYSTEM_PROMPT, build_contract_text, validate_result, _parse_json.
    """
    req = {
        "catalog": (requirements or {}).get("catalog", "quests"),
        "source_schema": (requirements or {}).get("source_schema", "public"),
        "target_schema": (requirements or {}).get("target_schema", "new_schema"),
    }

    # Передаём ПОЛНЫЙ контекст/пейлоад, как просил пользователь
    full_payload = {'ddl': payload.get('ddl', []),
                    "queries": payload.get("queries", [])}
    full_context = dict(context_pack or {})
    payload_qids = [
        str(q.get("queryid")) for q in full_payload["queries"] if q.get("queryid")
    ]

    user_text = build_contract_text(req, payload_qids, full_payload, full_context)

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_text},
    ]

    url = f"{BASE_URL}/chat/completions"
    last_text = ""

    for attempt in range(max_retries + 1):
        body = {
            "model": MODEL,  # для OSS — либо "gpt-oss-120b", либо "gpt://<folder>/gpt-oss-120b"
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
            "messages": messages,
        }
        resp = _session.post(
            url,
            headers=HEADERS,
            json=body,
            timeout=(YC_TIMEOUT_CONNECT, YC_TIMEOUT_READ),
        )
        if resp.status_code >= 400:
            # Сохраняем тело ошибки для диагностики
            try:
                with open("yandex_http_error.txt", "w", encoding="utf-8") as f:
                    f.write(f"status={resp.status_code}\nmodel={MODEL}\n\n{resp.text}")
            except Exception:
                pass
            resp.raise_for_status()

        jr = resp.json()
        try:
            last_text = jr["choices"][0]["message"]["content"]
        except Exception:
            last_text = json.dumps(jr, ensure_ascii=False)

        parsed = _parse_json(last_text)
        if parsed is not None:
            # Гарантируем наличие массивов
            for k in ("ddl", "migrations", "queries"):
                if k not in parsed or not isinstance(parsed.get(k), list):
                    parsed[k] = []

            # НОРМАЛИЗАЦИЯ: некоторые ответы кладут SQL в "statement" внутри queries[]
            fixed_queries: List[Dict[str, Any]] = []
            for item in parsed.get("queries", []):
                if isinstance(item, dict):
                    if "query" not in item and "statement" in item:
                        item["query"] = item.pop("statement")
                fixed_queries.append(item)
            parsed["queries"] = fixed_queries

            # Валидация контракта и анти-паттернов
            issues = validate_result(
                parsed, req["catalog"], req["source_schema"], req["target_schema"]
            )
            if not issues:
                return parsed

            # Мягкий ретрай с фидбеком
            fb = (
                "Нарушения контракта:\n- "
                + "\n- ".join(issues)
                + "\nИсправь и верни ПОЛНЫЙ JSON заново."
            )
            messages.append({"role": "user", "content": fb})
            continue

        # Если пришёл не-JSON — просим полный сырой JSON и пробуем ещё раз
        messages.append(
            {
                "role": "user",
                "content": "Верни полный СЫРОЙ JSON без Markdown/бэктиков.",
            }
        )

    # Не удалось получить валидный JSON
    with open("yandex_raw_response.txt", "w", encoding="utf-8") as f:
        f.write(last_text)
    raise ValueError("Модель не вернула валидный JSON; см. yandex_raw_response.txt")
