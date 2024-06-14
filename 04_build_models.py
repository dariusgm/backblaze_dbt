# build the dbt models
import os.path

import duckdb


def main():
    table_types = {}
    with duckdb.connect("dev.duckdb", read_only=True) as con:
        result = con.execute("SHOW TABLES").fetchall()
        # We need to determine the lowest common tables for doing a "UNION ALL"
        for pair in result:
            table = pair[0]
            print(f"Scanning {table}")
            types = set(map(lambda x: x[0], con.table(table).description))
            table_types[table] = types
    min_columns = ",".join(map(lambda x: f"'{x}'", sorted(list(set.intersection(*table_types.values())))))
    # process the data for the lowest common tables
    for table in table_types:
        with open(os.path.join("models",  "bronze", f"data_{table}.sql"), "w") as file:
            file.write(f"""SELECT {min_columns} FROM {table}_model""")


if __name__ == '__main__':
    main()
