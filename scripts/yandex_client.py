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
Только Trino SQL. Всегда полные имена <catalog>.<schema>.<table>. Пиши компактно.
Форматы хранения: Parquet/ORC предпочтительны, CSV/JSON — избегать на чтение.

DDL ПОЛИТИКА (ТОЛЬКО ICEBERG)
- Используй ТОЛЬКО Iceberg-свойства: WITH (format='PARQUET', partitioning=ARRAY[...]).
- НЕ ИСПОЛЬЗУЙ: bucketed_by, bucket_count, partitioned_by. Бакетинг — через 'bucket(col, 50)' внутри partitioning.
- Справочники (h_*) и небольшие S-таблицы: БЕЗ partitioning/бакетинга.

SCHEMA-AWARE ПРАВИЛО (ОЧЕНЬ ВАЖНО)
- Добавляй в partitioning ТОЛЬКО те колонки, которые ТОЧНО существуют в исходной таблице из {catalog}.{source_schema}.
- Если не уверен, что колонка существует (например, 'payment_dt' в линках) — НЕ используй её в partitioning.
- Если нужен date-pruning, но даты нет в таблице — используй «денормализацию»:
  Создай CTAS через JOIN с таблицей, где дата есть (обычно l_payment_client), добавь поле в схему и далее мигрируй с JOIN. (См. пример ниже.)

РЕКОМЕНДУЕМЫЕ PATTERN’Ы ДЛЯ PARTITIONING (если поле реально существует):
- Факты/линки платежей, где есть payment_dt и join по payment_id: partitioning = ARRAY['payment_dt', 'bucket(payment_id, 50)'].
- Если в линке НЕТ payment_dt — оставляй ТОЛЬКО 'bucket(payment_id, 50)' (без даты).
- Линки по сущностям:
  • l_excursion_author / l_excursion_category → ARRAY['bucket(excursion_id, 50)']
  • l_author_quest / l_quest_category / l_quest_episode → ARRAY['bucket(quest_id, 50)']

PARTITION PRUNING
- В WHERE нельзя применять функции к партиционным полям (month(), date_trunc()). Только диапазоны (>=, <).
  GOOD: WHERE payment_dt >= DATE '2024-06-01' AND payment_dt < DATE '2024-07-01'

ИЗБИРАТЕЛЬНЫЕ СТОЛБЦЫ
- Никаких SELECT * из физических таблиц. Допустимо только:
  (a) из локального CTE; (b) в семплере с LIMIT N (ORDER BY random() опционально).

РАННИЕ ФИЛЬТРЫ И ДЕКОМПОЗИЦИЯ
- Продвигай WHERE до JOIN/AGG (predicate pushdown, dynamic filtering).
- Разбивай тяжёлые пайплайны на шаги (CTE/промежуточные таблицы).

JOIN-Ы
- Избегай CROSS JOIN без ON.
- Большая слева (probe), малая справа (build). Малую можно BROADCAST.
- Горячие ключи джойна → 'bucket(<stable_id>, 50)'.

АГРЕГАЦИИ
- date_trunc(...) в SELECT/GROUP BY, НО НЕ в WHERE.
- COUNT(DISTINCT ...) дорого; по возможности approx_distinct(...).
- Условные средние: агрегаты с FILTER (например, AVG(val) FILTER (WHERE cond)) или AVG(IF(cond, val, NULL)).

ORDER BY / СЕМПЛЕРЫ
- ORDER BY без LIMIT — избегать.
- ORDER BY random() — только в семплере с LIMIT (не в боевом запросе).

JSON-КОНТРАКТ (КЛЮЧИ)
- ddl[]: {"statement": "..."}; migrations[]: {"statement": "..."}.
- queries[]: {"queryid":"...", "query":"..."} (НЕ "statement").
"""


# =========================
# SYSTEM_PROMPT (строго: только JSON)
# =========================
SYSTEM_PROMPT = f"""
Ты — LLM-копилот оптимизации SQL для Trino (Data Lakehouse).
Верни ТОЛЬКО СЫРОЙ JSON строго по схеме: {{ "ddl": [...], "migrations": [...], "queries": [...] }}.
Никакого Markdown, бэктиков и пояснительных текстов. Не добавляй лишних ключей.

Следуй Best Practices и примерам ниже. Соблюдай диалект Trino.

СТРОГОЕ ТРЕБОВАНИЕ К ФОРМАТУ JSON:
- Верни ТОЛЬКО валидный JSON без Markdown/бэктиков.
- Ключи строго: ddl[].statement, migrations[].statement, queries[].(queryid, query).

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
МИНИ-ПРИМЕРЫ (следуй стилю):

[DDL — ТОЛЬКО ICEBERG]
- CREATE SCHEMA:
  {"statement": "CREATE SCHEMA {catalog}.{target_schema}"}

- H-таблица без партиционирования/бакетинга:
  {"statement": "CREATE TABLE {catalog}.{target_schema}.h_client
                 WITH (format='PARQUET')
                 AS SELECT * FROM {catalog}.{source_schema}.h_client WHERE 1=0"}

- Линк с payment_dt ЕСТЬ в исходной таблице:
  {"statement": "CREATE TABLE {catalog}.{target_schema}.l_payment_client
                 WITH (format='PARQUET',
                       partitioning = ARRAY['payment_dt', 'bucket(payment_id, 50)'])
                 AS SELECT * FROM {catalog}.{source_schema}.l_payment_client WHERE 1=0"}

- Линк без payment_dt → ТОЛЬКО бакет по payment_id:
  {"statement": "CREATE TABLE {catalog}.{target_schema}.l_quest_payment
                 WITH (format='PARQUET',
                       partitioning = ARRAY['bucket(payment_id, 50)'])
                 AS SELECT * FROM {catalog}.{source_schema}.l_quest_payment WHERE 1=0"}

- (Опционально) Денормализация даты в линк (если нужен date-pruning):
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

[Миграции — перечисляй колонки явно]
  {"statement": "INSERT INTO {catalog}.{target_schema}.l_payment_client (payment_id, client_id, payment_dt, is_repeat_purchase)
                 SELECT payment_id, client_id, payment_dt, is_repeat_purchase
                 FROM {catalog}.{source_schema}.l_payment_client"}

  {"statement": "INSERT INTO {catalog}.{target_schema}.l_quest_payment (payment_id, quest_id)
                 SELECT payment_id, quest_id
                 FROM {catalog}.{source_schema}.l_quest_payment"}

  -- Для денормализации даты:
  {"statement": "INSERT INTO {catalog}.{target_schema}.l_excursion_payment (payment_id, excursion_id, payment_dt)
                 SELECT lep.payment_id, lep.excursion_id, lpc.payment_dt
                 FROM {catalog}.{source_schema}.l_excursion_payment lep
                 JOIN {catalog}.{source_schema}.l_payment_client   lpc
                   ON lep.payment_id = lpc.payment_id"}

[Фильтры по дате для pruning]
- ПЛОХО: WHERE month(payment_dt) = 6
- ХОРОШО: WHERE payment_dt >= DATE '2024-06-01' AND payment_dt < DATE '2024-07-01'

[Кварталы с диапазонами]
  WHERE ts >= TIMESTAMP '2024-01-01' AND ts < TIMESTAMP '2024-07-01'
  SELECT CASE
           WHEN ts >= TIMESTAMP '2024-01-01' AND ts < TIMESTAMP '2024-04-01' THEN 'Q1'
           WHEN ts >= TIMESTAMP '2024-04-01' AND ts < TIMESTAMP '2024-07-01' THEN 'Q2'
         END AS quarter, SUM(x)
  FROM ...
  GROUP BY 1

[EXISTS вместо IN]
- ПЛОХО: WHERE user_id IN (SELECT user_id FROM {catalog}.{source_schema}.events WHERE ...)
- ХОРОШО: WHERE EXISTS (SELECT 1 FROM {catalog}.{source_schema}.events e WHERE e.user_id = f.user_id AND ...)

[queries: ключи]
- ПРАВИЛЬНО:   {"queryid":"abc","query":"SELECT ..."}
- НЕПРАВИЛЬНО: {"queryid":"abc","statement":"SELECT ..."}
""".strip()

    CONTRACT = f"""
КОНТРАКТ (строго):
- catalog: {req['catalog']}, source_schema: {req['source_schema']}, target_schema: {req['target_schema']}.
- DDL: первой строкой всегда CREATE SCHEMA {req['catalog']}.{req['target_schema']}.
- MIGRATIONS: только INSERT/CTAS из {req['catalog']}.{req['source_schema']} → {req['catalog']}.{req['target_schema']}.
- QUERIES: ссылаются ТОЛЬКО на {req['catalog']}.{req['target_schema']}.* (старую схему НЕ использовать).
- Анти-паттерны запрещены:
  • SELECT * из физических таблиц (разрешено только из CTE или в семплере с LIMIT).
  • Функции month()/date_trunc() в WHERE (используй диапазоны).
  • Не делай ORDER BY random() в финальном запросе (только в семплере с LIMIT).
- Сохраняй queryid из PAYLOAD при переписывании.
- Верни ПОЛНЫЙ ВАЛИДНЫЙ JSON одной порцией. Никаких пояснений/Markdown.

ДОПОЛНИТЕЛЬНО ОБЯЗАТЕЛЬНО:
- Просканируй payload и выпиши ПОЛНЫЙ список исходных таблиц из {req['catalog']}.{req['source_schema']}.*, которые встречаются в FROM/JOIN.
- Для каждой из них создай объект в catalog.target_schema (Parquet; для фактов/линков — partitioning по дате, при горячих джойнах — bucket по стабильному ключу) и добавь миграцию INSERT (с явным списком колонок).
- В queries[] сохраняй исходный queryid и используй ключ "query" (НЕ "statement").
- Избегай ORDER BY random(), CROSS JOIN (VALUES ...), дублирующих UNION ALL; не применяй функции дат в WHERE.
- Если семантика метрики — пользователи, используй COUNT(DISTINCT client_id). Если кардинальность большая — approx_distinct.
- Если не уверена в распределении дат для кварталов — используй диапазоны (>=, <) в WHERE и CASE в SELECT, а не date_trunc в WHERE.

DDL ПОЛИТИКА (ЖЁСТКО, SCHEMA-AWARE):
- Используй ТОЛЬКО ICEBERG-стиль: WITH (format='PARQUET', partitioning=ARRAY[...]); НЕ использовать bucketed_by, bucket_count, partitioned_by.
- Перед созданием DDL для каждой таблицы из {req['catalog']}.{req['source_schema']}.* определи список ЕЁ колонок на основе payload/контекста:
  • Разрешено включать в partitioning ТОЛЬКО те поля, которые гарантированно существуют в этой таблице (например, payment_dt только если поле точно есть).
  • Если не уверен — не включай поле в partitioning.
- Если нужен date-pruning, но дата отсутствует в источнике, используй денормализацию:
  • CTAS через JOIN с таблицей, где дата есть (обычно l_payment_client), добавь поле в схему (WHERE 1=0).
  • Затем миграция INSERT с JOIN, чтобы заполнить денормализованное поле.
- Рекомендации:
  • l_*_payment / l_payment_client / l_payment_promo: ARRAY['payment_dt', 'bucket(payment_id, 50)'] ТОЛЬКО если payment_dt точно существует. Иначе — ARRAY['bucket(payment_id, 50)'].
  • l_excursion_author / l_excursion_category: ARRAY['bucket(excursion_id, 50)'].
  • l_author_quest / l_quest_category / l_quest_episode: ARRAY['bucket(quest_id, 50)'].
  • h_* и маленькие s_*: без partitioning/бакетинга.
- В migrations[] всегда указывай явные списки колонок (не SELECT *).
- DDL первым всегда CREATE SCHEMA {req['catalog']}.{req['source_schema']}. QUERIES должны ссылаться только на {req['catalog']}.{req['target_schema']}.*.

PAYLOAD DDL (полный):
{json.dumps(full_payload['ddl'], ensure_ascii=False)}

PAYLOAD QUERIES (полный):
{json.dumps(full_payload['queries'], ensure_ascii=False)}

CONTEXT PACK FIELDS EXPLANATION:


CONTEXT PACK (полный):
{json.dumps(full_context, ensure_ascii=False)}

ПРИМЕРЫ/ЯКОРЯ:
{EXAMPLES}
""".strip()
    return CONTRACT


# =========================
# Регэкспы и проверки
# =========================
CODE_FENCE_RE = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.S | re.I)
SELECT_STAR_FROM_RE = re.compile(
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
