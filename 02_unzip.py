# this script will unpack the previously downloaded data
import os
import zipfile

from shared import local_path, range_beginnng, range_end


def unzip(url):
    """
    Unpack the data
    """

    lp = local_path(url)
    local_directory = lp.replace(".zip", "").strip()
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


def main():
    os.makedirs("data", exist_ok=True)
    for year in range(range_beginnng, range_end):
        for quarter in range(1, 5):
            url = f"https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q{quarter}_{year}.zip"
            unzip(url)


if __name__ == '__main__':
    main()
