# sql_static.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple, Optional, Set, Iterable
from collections import Counter, defaultdict

import networkx as nx
import sqlglot
from sqlglot import expressions as E


# =========================
# Data models
# =========================


@dataclass(frozen=True)
class TableRef:
    catalog: Optional[str]  # допускаем None для 2-частных имён
    schema: str
    table: str

    @property
    def fqtn(self) -> str:
        parts = [self.catalog, self.schema, self.table]
        return ".".join([p for p in parts if p])


@dataclass
class QueryStat:
    queryid: str
    query: str
    runquantity: int


@dataclass
class DDLStmt:
    statement: str


@dataclass
class ContextPack:
    # inputs
    default_catalog: Optional[str]
    default_schema: Optional[str]
    ddl_tables: List[Dict]
    queries_overview: Dict
    # derived
    join_graph_edges: List[Tuple[str, str, int]]
    join_key_freq: Dict[str, int]
    table_scan_freq: Dict[str, int]
    table_scan_query_freq: Dict[str, int]
    column_usage_freq: Dict[str, int]
    groupby_patterns: List[Dict]
    window_functions: Dict[str, int]
    top_queries_by_q: List[Dict]
    # strategy hints
    hot_join_cliques: List[List[str]]
    hot_columns_per_table: Dict[str, List[str]]


# =========================
# Helpers
# =========================

_ID_QUOTE = re.compile(r'["`]')
# физическое имя как 2-частное (schema.table) ИЛИ 3-частное (catalog.schema.table)
PHYS_RX = re.compile(r"^([A-Za-z0-9_]+\.)?[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


def _is_phys(name: str) -> bool:
    return bool(PHYS_RX.match((name or "").strip()))


def _normalize_identifier(x: str) -> str:
    return _ID_QUOTE.sub("", (x or "").strip())


def _split_qualified(
    parts: List[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    ["catalog","schema","table"] -> (cat, sch, tab)
    ["schema","table"] -> (None, sch, tab)
    ["table"] -> (None, None, tab)
    """
    if not parts:
        return None, None, None
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, parts[0]


def _alias_name(expr) -> Optional[str]:
    """Безопасно достаёт строковое имя алиаса из узлов sqlglot (Table/Subquery/...)."""
    try:
        a = getattr(expr, "alias", None)
        if not a:
            return None
        if isinstance(a, str):
            return _normalize_identifier(a)
        if isinstance(a, E.Identifier):
            return _normalize_identifier(a.name)
        if isinstance(a, E.Alias):
            if isinstance(a.this, E.Identifier):
                return _normalize_identifier(a.this.name)
            if hasattr(a, "alias") and isinstance(a.alias, E.Identifier):
                return _normalize_identifier(a.alias.name)
        if hasattr(a, "name") and isinstance(a.name, str):
            return _normalize_identifier(a.name)
    except Exception:
        pass
    return None


def _make_tabref_from_expr(
    expr: E.Expression, default_catalog: Optional[str], default_schema: Optional[str]
) -> Optional[TableRef]:
    """
    Корректно извлекаем catalog/schema/table ТОЛЬКО из свойств sqlglot.Table:
    - expr.this  -> table (Identifier)
    - expr.db    -> schema (строка)
    - expr.catalog -> catalog (строка)
    Поддерживаем 2- и 3-частные имена.
    """
    if not isinstance(expr, E.Table):
        return None

    # table
    t = None
    if isinstance(expr.this, E.Identifier):
        t = _normalize_identifier(expr.this.name)
    elif expr.this is not None:
        try:
            t = _normalize_identifier(str(expr.this))
        except Exception:
            t = None

    # schema / catalog
    s = _normalize_identifier(expr.db) if getattr(expr, "db", None) else None
    c = _normalize_identifier(expr.catalog) if getattr(expr, "catalog", None) else None

    # дефолты
    s = s or (default_schema or None)
    c = c or (default_catalog or None)

    if t and s:
        return TableRef(c, s, t)
    return None


def _extract_default_catalog_schema_from_ddl(
    ddl_list: List["DDLStmt"],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Возвращает (catalog, schema) из ПЕРВОГО встреченного CREATE TABLE.
    Пример: CREATE TABLE quests.public.h_author (...) -> ('quests', 'public')
    """

    for d in ddl_list:
        stmt = getattr(d, "statement", None)
        if not stmt:
            continue
        try:
            tree = sqlglot.parse_one(stmt, read="trino")
        except Exception:
            continue

        # Берём первый CREATE (TABLE)
        create = next(tree.find_all(E.Create), None)
        if not create:
            continue

        catalog, schema = str(create.this).split('.')[:2]
        return catalog, schema

    return None, None


# =========================
# Input normalizers
# =========================


def _to_ddl_list(ddl_raw: List[Dict]) -> List[DDLStmt]:
    """Accept {statement|ddl|sql: "<CREATE ...>"}; ignore extras."""
    out: List[DDLStmt] = []
    for d in ddl_raw:
        if not isinstance(d, dict):
            continue
        stmt = d.get("statement") or d.get("ddl") or d.get("sql")
        if not stmt or not isinstance(stmt, str):
            continue
        out.append(DDLStmt(statement=stmt))
    return out


def _to_query_stat_list(queries_raw: List[Dict]) -> List[QueryStat]:
    """
    Accept aliases:
      id: queryid|queryId|id|qid
      text: query (fallback "SELECT 1")
      freq: runquantity|runQuantity|run_count (fallback 1)
    """
    out: List[QueryStat] = []
    for q in queries_raw:
        if not isinstance(q, dict):
            continue
        qid = q.get("queryid") or q.get("queryId") or q.get("id") or q.get("qid")
        if not qid:
            continue
        query_txt = q.get("query")
        if not isinstance(query_txt, str) or not query_txt.strip():
            query_txt = "SELECT 1"
        rq = q.get("runquantity") or q.get("runQuantity") or q.get("run_count") or 1
        try:
            rq = int(rq)
        except Exception:
            rq = 1
        out.append(QueryStat(queryid=str(qid), query=query_txt, runquantity=rq))
    return out


# =========================
# Core: CTE/Subquery resolver
# =========================


def _bases_of_any(
    expr: E.Expression, default_catalog: Optional[str], default_schema: Optional[str]
) -> List[str]:
    """
    Recursively return base tables (FIZ имена, если доступны).
    Возвращает сырые имена CTE/алиасов/короткие, если физическое имя не извлечь — их развернём позже.
    """
    bases: List[str] = []

    if isinstance(expr, E.Table):
        tr = _make_tabref_from_expr(expr, default_catalog, default_schema)
        if tr:
            bases.append(tr.fqtn)
        else:
            try:
                if isinstance(expr.this, E.Identifier):
                    bases.append(_normalize_identifier(expr.this.name))
            except Exception:
                pass
        return bases

    if isinstance(expr, E.Subquery):
        inner = expr.this
        if isinstance(inner, E.Select):
            bases.extend(
                _list_base_tables_from_select(inner, default_catalog, default_schema)
            )
        return bases

    if isinstance(expr, E.Select):
        bases.extend(
            _list_base_tables_from_select(expr, default_catalog, default_schema)
        )
        return bases

    if isinstance(expr, E.Alias):
        if expr.this is not None:
            bases.extend(_bases_of_any(expr.this, default_catalog, default_schema))
        return bases

    if isinstance(expr, E.Paren):
        if expr.this is not None:
            bases.extend(_bases_of_any(expr.this, default_catalog, default_schema))
        return bases

    if isinstance(expr, E.Join):
        if expr.args.get("this") is not None:
            bases.extend(
                _bases_of_any(expr.args["this"], default_catalog, default_schema)
            )
        if expr.args.get("expression") is not None:
            bases.extend(
                _bases_of_any(expr.args["expression"], default_catalog, default_schema)
            )
        if expr.args.get("on") is not None:
            bases.extend(
                _bases_of_any(expr.args["on"], default_catalog, default_schema)
            )
        return bases

    if isinstance(expr, E.Set):
        if expr.left is not None:
            bases.extend(_bases_of_any(expr.left, default_catalog, default_schema))
        if expr.right is not None:
            bases.extend(_bases_of_any(expr.right, default_catalog, default_schema))
        return bases

    # предикаты и выражения просто обходим на предмет вложенных Select/Column
    for child in expr.args.values():
        if isinstance(child, E.Expression):
            bases.extend(_bases_of_any(child, default_catalog, default_schema))
        elif isinstance(child, list):
            for c in child:
                if isinstance(c, E.Expression):
                    bases.extend(_bases_of_any(c, default_catalog, default_schema))

    return bases


def _list_base_tables_from_select(
    sel: E.Select, default_catalog: Optional[str], default_schema: Optional[str]
) -> List[str]:
    """Collect all base tables referenced inside a Select."""
    bases: List[str] = []
    from_expr = sel.args.get("from")
    if from_expr:
        for src in from_expr.find_all(
            (E.Table, E.Subquery, E.Select, E.Alias, E.Paren)
        ):
            bases.extend(_bases_of_any(src, default_catalog, default_schema))
    for j in sel.args.get("joins", []) or []:
        bases.extend(_bases_of_any(j, default_catalog, default_schema))
    # также пробежимся по WHERE (на случай неявных ссылок)
    where_expr = sel.args.get("where")
    if where_expr:
        bases.extend(_bases_of_any(where_expr, default_catalog, default_schema))
    return bases


def _resolve_alias_maps(
    tree: E.Expression, default_catalog: Optional[str], default_schema: Optional[str]
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """
    Build:
      - cte_map: CTE name -> list of base names (FQTN/alias/raw)
      - alias_map: alias -> list of base names
    """
    cte_map: Dict[str, List[str]] = {}
    alias_map: Dict[str, List[str]] = {}

    # 1) CTEs
    for cte in tree.find_all(E.CTE):
        id_expr = cte.find(E.Identifier)
        subq = cte.find(E.Subquery) or cte.find(E.Select)
        if id_expr is None or subq is None:
            continue
        name = _normalize_identifier(id_expr.name)
        sel = subq.this if isinstance(subq, E.Subquery) else subq
        if isinstance(sel, E.Select):
            bases = _list_base_tables_from_select(sel, default_catalog, default_schema)
            bases = list(dict.fromkeys(bases))  # dedup
            if bases:
                cte_map[name] = bases

    # 2) FROM/CTE/Table aliases
    for tbl in tree.find_all(E.Table):
        tr = _make_tabref_from_expr(tbl, default_catalog, default_schema)
        a = _alias_name(tbl)
        if tr and a:
            alias_map[a] = [tr.fqtn]
            continue
        # CTE/алиас референс
        name = None
        try:
            if isinstance(tbl.this, E.Identifier):
                name = _normalize_identifier(tbl.this.name)
        except Exception:
            name = None
        if name and name in cte_map:
            alias = a or name
            alias_map[alias] = list(cte_map[name])

    # 3) Subqueries with alias: (SELECT ...) AS x
    for sub in tree.find_all(E.Subquery):
        alias = _alias_name(sub)
        if not alias:
            continue
        inner = sub.this
        if isinstance(inner, E.Select):
            bases = _list_base_tables_from_select(
                inner, default_catalog, default_schema
            )
            bases = list(dict.fromkeys(bases))
            if bases:
                alias_map[alias] = bases

    return cte_map, alias_map


def _expand_to_physical(
    bases: List[str],
    alias_map: Dict[str, List[str]],
    cte_map: Dict[str, List[str]],
    short2fqtn: Dict[str, str],
) -> List[str]:
    """
    Разворачивает список баз (FQTN/CTE/алиас/короткое) в список **физических** имён (2 или 3 части).
    Приоритет: alias→CTE→short→phys-looking.
    """
    out: List[str] = []
    for b in bases:
        if not b:
            continue
        if b in alias_map:
            for x in alias_map[b]:
                if _is_phys(x):
                    out.append(x)
            continue
        if b in cte_map:
            for x in cte_map[b]:
                if _is_phys(x):
                    out.append(x)
            continue
        if b in short2fqtn:
            out.append(short2fqtn[b])
            continue
        if _is_phys(b):
            out.append(b)
    return list(dict.fromkeys(out))


def _iter_pairs(a: Iterable[str], b: Iterable[str]) -> Iterable[Tuple[str, str]]:
    for x in a:
        for y in b:
            if x != y:
                yield tuple(sorted((x, y)))


def _belongs_to_select(node: E.Expression, target_sel: E.Select) -> bool:
    """Проверяем, что ближайший предок типа Select — именно target_sel (чтобы не двойной счёт)."""
    p = node.parent
    while p is not None and not isinstance(p, E.Select):
        p = p.parent
    return p is target_sel


# ====== NEW: извлечение пар равенства колонок (для join-ключей и неявных join’ов) ======


def _walk_eq_pairs(expr: E.Expression) -> List[Tuple[E.Column, E.Column]]:
    """Возвращает все пары (Column, Column) из предикатов равенства в дереве expr."""
    pairs: List[Tuple[E.Column, E.Column]] = []
    for eq in expr.find_all(E.EQ):
        l, r = getattr(eq, "left", None), getattr(eq, "right", None)
        if isinstance(l, E.Column) and isinstance(r, E.Column):
            pairs.append((l, r))
    return pairs


def _col_phys_bases(
    col: E.Column,
    sel_alias_map_phys: Dict[str, List[str]],
    sel_bases_phys: List[str],
    short2fqtn: Dict[str, str],
) -> List[str]:
    """Вернуть физические базы для конкретной колонки в рамках SELECT."""
    # с префиксом?
    if col.table:
        name = _normalize_identifier(col.table)
        if name in sel_alias_map_phys:
            return sel_alias_map_phys[name]
        if name in short2fqtn:
            return [short2fqtn[name]]
        return []
    # без префикса — если в SELECT один физический источник, считаем его
    if len(sel_bases_phys) == 1:
        return list(sel_bases_phys)
    return []


def _join_key_pairs_for_select(
    sel: E.Select,
    alias_map_phys: Dict[str, List[str]],
    cte_map_phys: Dict[str, List[str]],
    short2fqtn: Dict[str, str],
    default_catalog: Optional[str],
    default_schema: Optional[str],
) -> List[Tuple[str, str, str, str]]:
    """
    Достаёт пары (A, B, colA, colB), где A/B — физические таблицы (schema.table|catalog.schema.table),
    colA/colB — имена колонок (в нижнем регистре).
    Источники: ON-условия у JOIN и предикаты WHERE.
    """
    res: List[Tuple[str, str, str, str]] = []

    # базы SELECT’а
    sel_bases_raw = _list_base_tables_from_select(sel, default_catalog, default_schema)
    sel_bases_phys = _expand_to_physical(
        sel_bases_raw, alias_map_phys, cte_map_phys, short2fqtn
    )

    # алиасы, видимые в этом SELECT
    sel_alias_map_phys: Dict[str, List[str]] = {}
    from_expr = sel.args.get("from")
    if from_expr:
        for t in from_expr.find_all(E.Table):
            a = _alias_name(t)
            if a:
                bases = alias_map_phys.get(a, [])
                if bases:
                    sel_alias_map_phys[a] = bases
    for j in sel.args.get("joins", []) or []:
        for t in j.find_all(E.Table):
            a = _alias_name(t)
            if a:
                bases = alias_map_phys.get(a, [])
                if bases:
                    sel_alias_map_phys[a] = bases

    # 1) JOIN ... ON ...
    for j in sel.args.get("joins", []) or []:
        on_expr = j.args.get("on")
        if not isinstance(on_expr, E.Expression):
            continue
        for lcol, rcol in _walk_eq_pairs(on_expr):
            lb = _col_phys_bases(lcol, sel_alias_map_phys, sel_bases_phys, short2fqtn)
            rb = _col_phys_bases(rcol, sel_alias_map_phys, sel_bases_phys, short2fqtn)
            if not lb or not rb:
                continue
            lname = _normalize_identifier(getattr(lcol, "name", "")).lower()
            rname = _normalize_identifier(getattr(rcol, "name", "")).lower()
            for a in lb:
                for b in rb:
                    if _is_phys(a) and _is_phys(b) and a != b:
                        aa, bb = sorted((a, b))
                        # нормализуем порядок колонок соответственно a/b
                        if aa == a:
                            res.append((aa, bb, lname, rname))
                        else:
                            res.append((aa, bb, rname, lname))

    # 2) Неявные join’ы через WHERE
    where_expr = sel.args.get("where")
    if isinstance(where_expr, E.Expression):
        for lcol, rcol in _walk_eq_pairs(where_expr):
            lb = _col_phys_bases(lcol, sel_alias_map_phys, sel_bases_phys, short2fqtn)
            rb = _col_phys_bases(rcol, sel_alias_map_phys, sel_bases_phys, short2fqtn)
            if not lb or not rb:
                continue
            lname = _normalize_identifier(getattr(lcol, "name", "")).lower()
            rname = _normalize_identifier(getattr(rcol, "name", "")).lower()
            for a in lb:
                for b in rb:
                    if _is_phys(a) and _is_phys(b) and a != b:
                        aa, bb = sorted((a, b))
                        if aa == a:
                            res.append((aa, bb, lname, rname))
                        else:
                            res.append((aa, bb, rname, lname))

    return res


# =========================
# Main analysis
# =========================


def build_context_pack(ddl: List[Dict], queries: List[Dict]) -> Dict:
    ddl_list = _to_ddl_list(ddl)
    q_list = _to_query_stat_list(queries)

    default_catalog, default_schema = _extract_default_catalog_schema_from_ddl(ddl_list)

    # --- Build short->fqtn from DDL AND from queries ---
    short2fqtn_counter: Dict[str, Counter] = defaultdict(Counter)

    # From DDL
    for d in ddl_list:
        try:
            tree = sqlglot.parse_one(d.statement, read="trino")
        except Exception:
            continue
        for ct in tree.find_all(E.Create):
            if not isinstance(ct.this, E.Table):
                continue
            ids = [
                _normalize_identifier(p.name) for p in ct.this.find_all(E.Identifier)
            ]
            c, s, t = _split_qualified(ids)
            if t:
                fq = ".".join([p for p in [c, s, t] if p])
                short2fqtn_counter[t][fq] += 1

    # From queries (physical tables spotted in AST)
    for q in q_list:
        try:
            tree = sqlglot.parse_one(q.query, read="trino")
        except Exception:
            continue
        for tbl in tree.find_all(E.Table):
            tr = _make_tabref_from_expr(tbl, default_catalog, default_schema)
            if tr:
                short2fqtn_counter[tr.table][tr.fqtn] += 1

    short2fqtn: Dict[str, str] = {}
    for t, cnt in short2fqtn_counter.items():
        short2fqtn[t] = cnt.most_common(1)[0][0]

    # 1) Tables from DDL (reference)
    ddl_tables: List[Dict] = []
    for d in ddl_list:
        try:
            tree = sqlglot.parse_one(d.statement, read="trino")
        except Exception:
            continue
        for ct in tree.find_all(E.Create):
            if not isinstance(ct.this, E.Table):
                continue
            ids = [
                _normalize_identifier(p.name) for p in ct.this.find_all(E.Identifier)
            ]
            c, s, t = _split_qualified(ids)
            if t:
                ddl_tables.append(
                    {
                        "catalog": c or default_catalog,
                        "schema": s or default_schema,
                        "table": t,
                    }
                )

    # 2) Walk queries
    table_scan_freq: Counter = Counter()
    table_scan_query_freq: Counter = Counter()
    column_usage_freq: Counter = Counter()
    join_edge_weight: Dict[Tuple[str, str], int] = defaultdict(int)
    join_key_freq: Counter = Counter()
    groupby_patterns: List[Dict] = []
    window_functions: Counter = Counter()

    for q in q_list:
        per_query_phys_tables: Set[str] = set()

        try:
            tree = sqlglot.parse_one(q.query, read="trino")
        except Exception:
            # fallback: FROM <phys>
            m = re.findall(
                r"\bFROM\s+(([A-Za-z0-9_]+\.)?[A-Za-z0-9_]+\.[A-Za-z0-9_]+)",
                q.query,
                flags=re.I,
            )
            for fq, _ in m:
                if _is_phys(fq):
                    table_scan_freq[fq] += q.runquantity
                    per_query_phys_tables.add(fq)
            # учтём разок на запрос
            for fq in per_query_phys_tables:
                table_scan_query_freq[fq] += q.runquantity
            continue

        # Resolve CTE + alias maps (raw)
        cte_map_raw, alias_map_raw = _resolve_alias_maps(
            tree, default_catalog, default_schema
        )

        # Normalize maps to physical where possible
        cte_map_phys = {
            k: _expand_to_physical(v, alias_map_raw, cte_map_raw, short2fqtn)
            for k, v in cte_map_raw.items()
        }
        alias_map_phys = {
            k: _expand_to_physical(v, alias_map_raw, cte_map_raw, short2fqtn)
            for k, v in alias_map_raw.items()
        }

        derived_name_map: Dict[str, List[str]] = {**cte_map_phys, **alias_map_phys}

        # --- Count scans: ONLY physical; derived names пушим на базы ---
        for tbl in tree.find_all(E.Table):
            tr = _make_tabref_from_expr(tbl, default_catalog, default_schema)
            if tr:
                table_scan_freq[tr.fqtn] += q.runquantity
                per_query_phys_tables.add(tr.fqtn)
                continue

            name = None
            try:
                if isinstance(tbl.this, E.Identifier):
                    name = _normalize_identifier(tbl.this.name)
            except Exception:
                name = None

            if name:
                bases = derived_name_map.get(name, [])
                if bases:
                    for fq in bases:
                        if _is_phys(fq):
                            table_scan_freq[fq] += q.runquantity
                            per_query_phys_tables.add(fq)
                elif name in short2fqtn:
                    fq = short2fqtn[name]
                    table_scan_freq[fq] += q.runquantity
                    per_query_phys_tables.add(fq)
                # не мапнули → игнор (не засоряем ярлыками)

        # учтём «разок на запрос» для всех встреченных физ. таблиц
        for fq in per_query_phys_tables:
            table_scan_query_freq[fq] += q.runquantity

        # === Column attribution per SELECT (to reduce __unknown__) ===
        for sel in tree.find_all(E.Select):
            # набор физ. источников в этом SELECT
            sel_bases_raw = _list_base_tables_from_select(
                sel, default_catalog, default_schema
            )
            sel_bases_phys = _expand_to_physical(
                sel_bases_raw, alias_map_phys, cte_map_phys, short2fqtn
            )
            sel_bases_phys_set = set(sel_bases_phys)

            # видимые в этом SELECT алиасы
            sel_alias_map_phys: Dict[str, List[str]] = {}
            from_expr = sel.args.get("from")
            if from_expr:
                for t in from_expr.find_all(E.Table):
                    a = _alias_name(t)
                    if a:
                        bases = alias_map_phys.get(a, [])
                        if bases:
                            sel_alias_map_phys[a] = bases
            for j in sel.args.get("joins", []) or []:
                for t in j.find_all(E.Table):
                    a = _alias_name(t)
                    if a:
                        bases = alias_map_phys.get(a, [])
                        if bases:
                            sel_alias_map_phys[a] = bases

            sel_single_alias_bases: Optional[List[str]] = None
            if len(sel_alias_map_phys) == 1:
                sel_single_alias_bases = list(sel_alias_map_phys.values())[0]

            # имя_колонки -> множ. баз, где она встречалась с префиксом в этом SELECT
            col_to_bases_prefixed: Dict[str, Set[str]] = defaultdict(set)

            # 1) префиксованные колонки
            for col in sel.find_all(E.Column):
                if not _belongs_to_select(col, sel):
                    continue
                col_name = _normalize_identifier(getattr(col, "name", None))
                if not col_name:
                    continue
                if col.table:
                    bases = sel_alias_map_phys.get(_normalize_identifier(col.table), [])
                    if not bases:
                        short_base = short2fqtn.get(_normalize_identifier(col.table))
                        bases = [short_base] if short_base else []
                    if bases:
                        for fq in bases:
                            if _is_phys(fq):
                                column_usage_freq[f"{fq}.{col_name}"] += q.runquantity
                                col_to_bases_prefixed[col_name].add(fq)

            # 2) непрефиксованные — три правила атрибуции
            for col in sel.find_all(E.Column):
                if not _belongs_to_select(col, sel):
                    continue
                if col.table:
                    continue
                col_name = _normalize_identifier(getattr(col, "name", None))
                if not col_name:
                    continue

                if len(sel_bases_phys_set) == 1:
                    only_base = next(iter(sel_bases_phys_set))
                    column_usage_freq[f"{only_base}.{col_name}"] += q.runquantity
                elif len(col_to_bases_prefixed[col_name]) == 1:
                    only_base = next(iter(col_to_bases_prefixed[col_name]))
                    column_usage_freq[f"{only_base}.{col_name}"] += q.runquantity
                elif sel_single_alias_bases:
                    for fq in sel_single_alias_bases:
                        column_usage_freq[f"{fq}.{col_name}"] += q.runquantity
                else:
                    column_usage_freq[f"__unknown__.{col_name}"] += q.runquantity

            # GROUP BY по этому SELECT (raw + columns_only)
            gb_raw: List[str] = []
            gb_cols_only: List[str] = []
            for gb in sel.find_all(E.Group):
                exprs = gb.args.get("expressions", []) or []
                for e in exprs:
                    try:
                        gb_raw.append(e.sql(dialect="trino"))
                    except Exception:
                        gb_raw.append(str(e))
                    if isinstance(e, E.Column):
                        nm = (
                            _normalize_identifier(e.name)
                            if getattr(e, "name", None)
                            else None
                        )
                        if nm:
                            gb_cols_only.append(nm)
            if gb_raw:
                groupby_patterns.append(
                    {
                        "queryid": q.queryid,
                        "runquantity": q.runquantity,
                        "columns_raw": gb_raw,
                        "columns_only": gb_cols_only,
                    }
                )

            # === JOIN-ключи (и рёбра на их основе) для этого SELECT ===
            key_pairs = _join_key_pairs_for_select(
                sel,
                alias_map_phys,
                cte_map_phys,
                short2fqtn,
                default_catalog,
                default_schema,
            )
            for a, b, ca, cb in key_pairs:
                if _is_phys(a) and _is_phys(b) and a != b:
                    aa, bb = sorted((a, b))
                    join_edge_weight[(aa, bb)] += q.runquantity
                    key = f"{aa}|{bb}|{ca}|{cb}"
                    join_key_freq[key] += q.runquantity

        # Window functions (глобально по запросу)
        for win in tree.find_all(E.Window):
            func = win.this
            name = None
            try:
                name = getattr(func, "name", None)
            except Exception:
                name = None
            name = (name or "").upper()
            if name:
                window_functions[name] += q.runquantity

    # 3) Build join graph and extract hot cliques (physical only)
    G = nx.Graph()
    for (a, b), w in join_edge_weight.items():
        if a and b and a != b and _is_phys(a) and _is_phys(b):
            if G.has_edge(a, b):
                G[a][b]["weight"] += w
            else:
                G.add_edge(a, b, weight=w)

    hot_edges = sorted(
        ((a, b), w)
        for (a, b), w in join_edge_weight.items()
        if _is_phys(a) and _is_phys(b)
    )
    hot_edges = sorted(hot_edges, key=lambda kv: kv[1], reverse=True)[:5]
    hot_nodes: Set[str] = set()
    for (a, b), _ in hot_edges:
        hot_nodes.update([a, b])
    sub = G.subgraph(hot_nodes).copy()
    cliques = [c for c in nx.find_cliques(sub)]
    cliques = sorted(cliques, key=len, reverse=True)[:3]

    # 4) Hot columns per table (top-10), только физические имена
    hot_columns_per_table: Dict[str, List[str]] = defaultdict(list)
    by_table: Dict[str, Counter] = defaultdict(Counter)
    for key, freq in column_usage_freq.items():
        if "." not in key:
            continue
        t, col = key.rsplit(".", 1)
        if t == "__unknown__" or not _is_phys(t):
            continue
        by_table[t][col] += freq
    for t, cnt in by_table.items():
        hot_columns_per_table[t] = [c for c, _ in cnt.most_common(10)]

    # 5) Top queries by runquantity
    top_queries = sorted(q_list, key=lambda x: x.runquantity, reverse=True)[:10]
    top_queries_by_q = [
        {"queryid": q.queryid, "runquantity": q.runquantity} for q in top_queries
    ]

    # 6) Normalize table_scan_freq to physical only
    table_scan_freq_phys: Dict[str, int] = defaultdict(int)
    for k, v in table_scan_freq.items():
        if _is_phys(k):
            table_scan_freq_phys[k] += int(v)
        else:
            mapped = short2fqtn.get(k)
            if mapped and _is_phys(mapped):
                table_scan_freq_phys[mapped] += int(v)

    # 7) table_scan_query_freq (уникально на запрос)
    table_scan_query_freq_phys: Dict[str, int] = {
        k: int(v) for k, v in table_scan_query_freq.items() if _is_phys(k)
    }

    context = ContextPack(
        default_catalog=default_catalog,
        default_schema=default_schema,
        ddl_tables=ddl_tables,
        queries_overview={
            "total_queries": len(q_list),
            "total_runquantity": sum(q.runquantity for q in q_list),
        },
        join_graph_edges=[
            (a, b, int(w))
            for (a, b), w in join_edge_weight.items()
            if _is_phys(a) and _is_phys(b)
        ],
        join_key_freq=dict(join_key_freq),
        table_scan_freq=dict(table_scan_freq_phys),
        table_scan_query_freq=table_scan_query_freq_phys,
        column_usage_freq=dict(column_usage_freq),
        groupby_patterns=groupby_patterns,
        window_functions=dict(window_functions),
        top_queries_by_q=top_queries_by_q,
        hot_join_cliques=cliques,
        hot_columns_per_table=dict(hot_columns_per_table),
    )

    return json.loads(json.dumps(asdict(context)))
