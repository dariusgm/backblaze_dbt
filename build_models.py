# build the dbt models
import duckdb


def main():
    table_types = {}
    with duckdb.connect("dev.duckdb", read_only=True) as con:
        result = con.execute("SHOW TABLES").fetchall()
        # We need to determine the lowest common tables for doing a "UNION ALL"
        for pair in result:
            table = pair[0]
            print(f"Building dbt model for {table}")
            set(map(lambda x: x[0], con.table(table).description))
            con.table(table)
            con.execute(f"CREATE MODEL {table}_model AS (SELECT * FROM {table})")


if __name__ == '__main__':
    main()