# this script will download the raw data from the internet
import os

import requests

from shared import range_beginnng, range_end, local_path


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


def main():
    os.makedirs("data", exist_ok=True)
    for year in range(range_beginnng, range_end):
        for quarter in range(1, 5):
            url = f"https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q{quarter}_{year}.zip"
            try:
                download_data(url)
            except Exception as e:
                continue


if __name__ == '__main__':
    main()
