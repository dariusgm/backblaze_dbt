# this script will download the data from backblaze
# and save it to the local disk
# besides that, everything else is done in dbt
import glob

import duckdb
import pandas as pd


def main():

    # prepare processing pipeline
    target_duckdb_file = "dev.duckdb"
    with duckdb.connect(target_duckdb_file) as con:
        for g in glob.glob("data/**"):
            if "__MACOSX" in g:
                continue
            if ".zip" in g:
                continue

            _data, quarter = g.split("/")
            # create new for each quarter a table
            # with all days inside

            for file in glob.glob(f"data/{quarter}/*.csv"):
                try:
                    _data, quarter, timestamp = file.split("/")
                    timestamp = timestamp.replace(".csv", "").replace("-", "")
                    print(f"Processing data for quarter {quarter} and timestamp {timestamp}")
                    df = pd.read_csv(file, low_memory=False, dtype="str")
                    con.register("timestamp", df)
                    con.sql(f"CREATE TABLE IF NOT EXISTS data_{timestamp} AS SELECT * FROM timestamp")
                except Exception as e:
                    print(f"An error occurred while processing the data for quarter {quarter}: {e}")


if __name__ == '__main__':
    main()
