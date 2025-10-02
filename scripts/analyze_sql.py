#!/usr/bin/env python3
import sys, json, pathlib, re
from sql_static import build_context_pack

USAGE = "Usage: analyze_sql.py <payload.json>"

# ---------- строгий загрузчик ----------
def _load_json_strict(path: pathlib.Path) -> dict:
    raw = path.read_text(encoding="utf-8")
    return json.loads(raw)


# ---------- «расхлябанный» загрузчик для полусломанных файлов ----------
def _load_json_loose(path: pathlib.Path) -> dict:
    """
    Вытягивает полезное даже из очень «грязного» payload:
    - DDL: ищет полные имена таблиц в CREATE TABLE <catalog>.<schema>.<table>
           и синтезирует валидный DDL вида: CREATE TABLE c.s.t (x int)
    - Queries: вытаскивает пары (queryid, runquantity) и пытается взять "query".
               Если "query" порезан (есть '...' или незакрытые кавычки) — ставим "SELECT 1".
    """
    txt = path.read_text(encoding="utf-8", errors="replace")

    # 1) Соберём DDL-таблицы по сигнатуре CREATE TABLE <catalog>.<schema>.<table>
    ddl = []
    # Разрешим буквенно-цифровые имена, подчёркивания и точки/кавычки
    ddl_pattern = re.compile(
        r"CREATE\s+TABLE\s+([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)",
        flags=re.IGNORECASE,
    )
    seen_fqtn = set()
    for m in ddl_pattern.finditer(txt):
        cat, sch, tab = m.group(1), m.group(2), m.group(3)
        fqtn = f"{cat}.{sch}.{tab}"
        if fqtn in seen_fqtn:
            continue
        seen_fqtn.add(fqtn)
        # Синтезируем простой валидный DDL, которого sqlglot сможет понять
        ddl.append({"statement": f"CREATE TABLE {fqtn} (x int)"})

    # 2) Соберём Queries. Будем искать блоки с queryid и runquantity
    #    а "query" попытаемся вытащить, иначе подставим SELECT 1
    queries = []

    # Найдём все позиции queryid
    qid_iter = list(re.finditer(r'"queryid"\s*:\s*"([0-9a-fA-F-]{8,})"', txt))
    if not qid_iter:
        # fallback: иногда ключ называется "queryId"
        qid_iter = list(re.finditer(r'"queryId"\s*:\s*"([0-9a-fA-F-]{8,})"', txt))

    for i, qm in enumerate(qid_iter):
        qid = qm.group(1)
        start = qm.end()
        end = qid_iter[i + 1].start() if i + 1 < len(qid_iter) else len(txt)
        block = txt[start:end]

        # Попытка достать runquantity в пределах блока
        rq_match = re.search(
            r'"runquantity"\s*:\s*([0-9]+)', block, flags=re.IGNORECASE
        )
        runquantity = int(rq_match.group(1)) if rq_match else 1

        # Попытка вытащить query в блоке — простейшая форма "query":"...".
        q_match = re.search(r'"query"\s*:\s*"(.*?)"', block, flags=re.S)
        if q_match:
            qraw = q_match.group(1)
            # Если строка содержит явные обрезки/многоточия — подменим
            if "..." in qraw or qraw.strip() == "":
                query = "SELECT 1"
            else:
                # Уберём любые невалидные управляющие символы
                query = qraw.replace("\r", " ").replace("\n", " ")
        else:
            query = "SELECT 1"

        queries.append({"queryid": qid, "query": query, "runquantity": runquantity})

    # Если совсем ничего не нашли — пусть это будет валидный пустой payload
    return {"ddl": ddl, "queries": queries}


def _load_payload(path: pathlib.Path) -> dict:
    # Сначала строгая попытка
    try:
        return _load_json_strict(path)
    except json.JSONDecodeError:
        # Переходим в «loose mode»
        payload = _load_json_loose(path)
        # На всякий случай убедимся, что это сериализуется
        json.dumps(payload)
        return payload


def main():
    if len(sys.argv) != 2:
        print(USAGE)
        sys.exit(2)

    src = pathlib.Path(sys.argv[1])
    if not src.exists():
        print(f"[ERROR] File not found: {src}\n{USAGE}")
        sys.exit(2)

    payload = _load_payload(src)
    ddl = payload.get("ddl", [])
    queries = payload.get("queries", [])

    ctx = build_context_pack(ddl, queries)

    out = src.with_suffix(".context_pack.json")
    out.write_text(json.dumps(ctx, ensure_ascii=False, indent=2), encoding="utf-8")

    ov = ctx.get("queries_overview", {})
    print(f"OK: context_pack written to {out}")
    print(
        f"Total queries: {ov.get('total_queries')}, total runquantity: {ov.get('total_runquantity')}"
    )
    print("Top join edges (up to 5):")
    for a, b, w in sorted(
        ctx.get("join_graph_edges", []), key=lambda t: t[2], reverse=True
    )[:5]:
        print(f"  {a} — {b}: {w}")


if __name__ == "__main__":
    main()
