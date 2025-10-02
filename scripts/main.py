# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, json, os, pathlib, time, importlib.util
from typing import Any, Dict

HERE = pathlib.Path(__file__).resolve().parent


def load_json(p: str | os.PathLike):
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def import_module_from(path: pathlib.Path, modname: str):
    spec = importlib.util.spec_from_file_location(modname, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot import {modname} from {path}")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def try_import(name: str, fallback: str):
    try:
        return __import__(name)
    except Exception:
        path = HERE / fallback
        if path.exists():
            return import_module_from(path, name.replace(".", "_"))
        raise


def load_payload_loose(payload_path: pathlib.Path) -> Dict[str, Any]:
    try:
        analyze = try_import("analyze_sql", "analyze_sql.py")
        if hasattr(analyze, "_load_payload"):
            return analyze._load_payload(payload_path)  # type: ignore
    except Exception:
        pass
    return load_json(payload_path)


def build_context(ddl: list[dict], queries: list[dict]) -> Dict[str, Any]:
    sql_static = try_import("sql_static", "sql_static.py")
    return sql_static.build_context_pack(ddl=ddl, queries=queries)  # type: ignore


def main():
    ap = argparse.ArgumentParser(
        description="LLM tester with contract & best practices for Trino."
    )
    ap.add_argument("--provider", choices=["local", "yandex"], default="yandex")
    ap.add_argument("--payload", required=True, help="payload.json (raw workload)")
    ap.add_argument("--context", required=False, help="context_pack.json (optional)")
    ap.add_argument("--catalog", default="quests")
    ap.add_argument("--source-schema", dest="source_schema", default="public")
    ap.add_argument("--target-schema", dest="target_schema", default="new_schema")
    ap.add_argument("--force-build-context", action="store_true")
    ap.add_argument("--out", default=str(HERE / f"result-{int(time.time())}.json"))

    # сетевые настройки (можно менять флагами)
    ap.add_argument(
        "--t-connect", type=float, default=float(os.getenv("YC_TIMEOUT_CONNECT", "15"))
    )
    ap.add_argument(
        "--t-read", type=float, default=float(os.getenv("YC_TIMEOUT_READ", "240"))
    )
    ap.add_argument(
        "--http-retries", type=int, default=int(os.getenv("YC_HTTP_RETRIES", "4"))
    )
    ap.add_argument(
        "--backoff", type=float, default=float(os.getenv("YC_BACKOFF_FACTOR", "1.2"))
    )
    args = ap.parse_args()

    # прокинем в yandex_client через env — он их прочитает
    os.environ["YC_TIMEOUT_CONNECT"] = str(args.t_connect)
    os.environ["YC_TIMEOUT_READ"] = str(args.t_read)
    os.environ["YC_HTTP_RETRIES"] = str(args.http_retries)
    os.environ["YC_BACKOFF_FACTOR"] = str(args.backoff)

    payload = load_payload_loose(pathlib.Path(args.payload))
    ddl = payload.get("ddl", [])
    queries = payload.get("queries", [])
    if args.force_build_context or not args.context:
        context_pack = build_context(ddl, queries)
    else:
        context_pack = load_json(args.context)

    if args.provider == "local":
        provider = import_module_from(HERE / "local_provider.py", "local_provider")
        result = provider.call_local_llm(context_pack, payload)  # type: ignore
    else:
        provider = import_module_from(HERE / "yandex_client.py", "yandex_client")
        requirements = {
            "catalog": args.catalog,
            "source_schema": args.source_schema,
            "target_schema": args.target_schema,
        }
        result = provider.call_yandex(context_pack, payload, requirements=requirements)  # type: ignore

    out_path = pathlib.Path(args.out)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # мини-отчёт
    print("=== RESULT SUMMARY ===")
    print(f"Saved to: {out_path}")
    ddl_n = len(result.get("ddl", []))
    mig_n = len(result.get("migrations", []))
    q_n = len(result.get("queries", []))
    print(f"DDL: {ddl_n}, Migrations: {mig_n}, Queries: {q_n}")

    bad = []
    if ddl_n == 0:
        bad.append("no DDL")
    else:
        first = (result["ddl"][0]["statement"]).upper()
        expect = f"{args.catalog}.{args.target_schema}".upper()
        if not (first.startswith("CREATE SCHEMA") and expect in first):
            bad.append("first DDL is not CREATE SCHEMA <catalog>.<target_schema>")
    if mig_n == 0:
        bad.append("migrations is empty")
    if q_n == 0:
        bad.append("queries is empty (must be non-empty)")

    if bad:
        print("Diagnostics:")
        for x in bad:
            print(" -", x)
    else:
        print("OK by contract (superficial checks)")


if __name__ == "__main__":
    main()
