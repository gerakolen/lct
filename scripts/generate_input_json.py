import argparse
import json
import random
import string
import uuid


def random_table_name():
    return "".join(random.choices(string.ascii_lowercase, k=4))


def generate_ddl_entries(num_tables):
    ddl_entries = []
    table_names = []
    for _ in range(num_tables):
        table_name = random_table_name()
        table_names.append(table_name)
        # Randomly choose a schema
        if random.choice([True, False]):
            statement = f"CREATE TABLE {table_name} (id INT PRIMARY KEY, {table_name}_name VARCHAR(100))"
        else:
            statement = f"CREATE TABLE {table_name} (order_id INT PRIMARY KEY, user_id INT, amount DECIMAL)"
        ddl_entries.append({"statement": statement})
    return ddl_entries, table_names


def generate_query_entries(num_queries, table_names):
    queries = []
    for _ in range(num_queries):
        queryid = str(uuid.uuid4())
        runquantity = random.randint(100, 10000)
        # Reference random tables from the list
        if len(table_names) >= 2:
            t1, t2 = random.sample(table_names, 2)
            query = f"SELECT u.id, u.name, COUNT(o.order_id) FROM {t1} u JOIN {t2} o ON u.id = o.user_id GROUP BY u.id"
        else:
            t1 = table_names[0]
            query = f"SELECT * FROM {t1}"
        # Optionally add a CTE query
        if random.choice([True, False]) and len(table_names) >= 2:
            t1, t2 = random.sample(table_names, 2)
            query = f"WITH active_{t1} AS (SELECT id FROM {t1} WHERE active = true) SELECT * FROM {t2} WHERE user_id IN (SELECT id FROM active_{t1})"
        queries.append({"queryid": queryid, "query": query, "runquantity": runquantity})
    return queries


def main():
    parser = argparse.ArgumentParser(description="Generate JSON for DDL and queries.")
    parser.add_argument("--ddl", type=int, default=2, help="Number of DDL entries")
    parser.add_argument(
        "--queries", type=int, default=2, help="Number of query entries"
    )
    args = parser.parse_args()

    ddl_entries, table_names = generate_ddl_entries(args.ddl)
    queries = generate_query_entries(args.queries, table_names)

    result = {
        "url": "jdbc:postgresql://localhost:5432/mydb?login=admin&password=secret",
        "ddl": ddl_entries,
        "queries": queries,
    }

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
