import os

range_beginnng = 2021
range_end = 2025
url = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q{quarter}_{year}.zip"

def local_path(url):
    local_file_name = url.split("/")[-1]
    return os.path.join("data", local_file_name)

