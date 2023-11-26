from itertools import cycle
from urllib.parse import urlsplit
from zipfile import ZipFile, is_zipfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests as req

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    # "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    # "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    # "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    # "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    # "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    # "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


TARGET_DIR=Path('downloads/')
TARGET_DIR.mkdir(exist_ok=True)


def download_file(uri, dest_dir):
    filename = urlsplit(uri).path.split(sep='/')[-1]
    zip_path = dest_dir / filename 

    try:
        resp = req.get(uri, stream=True)

        if resp.status_code != 200:
            raise Exception(f'Returned status code: {resp.status_code}')

        with zip_path.open(mode='wb') as target:
            for chunk in resp.iter_content(chunk_size=4096):
                target.write(chunk)

        return (uri, True)
    except Exception as err:
        print(f'{uri}: {err}')

    return (uri, False)


def main():
    print('Job started')
    print('Downloading files ...')
    
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = []

        for uri in download_uris:
            f = pool.submit(download_file, uri=uri, dest_dir=TARGET_DIR)
            futures.append(f)

        for future in as_completed(futures):
            uri, completed = future.result()
            if completed:
                print(f'{uri} downloaded')
            else:
                print(f'{uri} failed')
            
    print('Extracting files ...')

    for path in sorted(TARGET_DIR.glob('*.zip')):
        try:
            if not is_zipfile(path):
                print(f'{path.name} is not a zip file')
                continue

            with ZipFile(path, mode='r') as zip:
                for csv in filter(lambda n: n.endswith('.csv'), zip.namelist() ):
                    zip.extract(csv, TARGET_DIR)

            print(f'{path.name} extracted')
        finally:
            path.unlink()
            
    print('Job completed')


if __name__ == "__main__":
    main()
