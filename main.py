# this script will download the data from backblaze
# and save it to the local disk
# besides that, everything else is done in dbt
import glob
import os
import zipfile

import duckdb
import requests
import pandas as pd

# download the data from backblaze
def download_data(url):
    r = requests.get(url, stream=True)
    lp = local_path(url)
    if not os.path.exists(lp):
        print(f"Downloading data from {url}")
        with open(lp, "wb") as file:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
        print(f"Data downloaded successfully from {url}")


def local_path(url):
    local_file_name = url.split("/")[-1]
    return os.path.join("data", local_file_name)


def unzip(url):
    """
    Unpack the data
    """

    lp = local_path(url)
    local_directory = lp.replace(".zip", "")
    if "2021" in url:
        target_directory = local_directory.replace(".zip", "")
    else:
        target_directory = "data"
    if not os.path.exists(target_directory):
        print(f"Data unzipping {lp} -> {target_directory}")
        # for 2021 and before, we need to add the year directory
        with zipfile.ZipFile(lp, "r") as zip_ref:
            zip_ref.extractall(target_directory)

        print(f"Data unzipped successfully to {target_directory} directory.")

def download():
    os.makedirs("data", exist_ok=True)
    for year in range(2021, 2025):
        for quarter in range(1, 5):
            url = f"https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q{quarter}_{year}.zip"
            try:
                download_data(url)
                unzip(url)
            except Exception as e:
                continue
def main():
    download()

    # prepare processing pipeline
    for g in glob.glob("data/**"):
        if "__MACOSX" in g:
            continue
        if ".zip" in g:
            continue

        _data, quarter = g.split("/")
        # create new for each quarter a table
        # with all days inside
        database_file = f"{quarter}.duckdb"
        if not os.path.exists(database_file):
            with duckdb.connect(database_file) as con:
                for file in glob.glob(f"{g}/*.csv"):
                    print(f"Processing {file}")
                    df = pd.read_csv(file, low_memory=False, dtype="str")
                    duckdb.sql(f"CREATE TABLE IF NOT EXISTS {quarter}  AS SELECT * FROM df")
                    duckdb.sql(f"INSERT INTO {quarter} SELECT * FROM df")

if __name__ == '__main__':
    main()
