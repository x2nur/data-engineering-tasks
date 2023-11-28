import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup as soup
import pandas as pd


BASE_URL = r'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
TIMESTAMP = r'2022-02-07 14:03'
TARGET_COL = 'HourlyDryBulbTemperature'


def download_file(uri, file_path):
    try:
        resp = requests.get(uri, stream=True)
        if resp.status_code != 200:
            raise Exception(f'Returned status code: {resp.status_code}')
        with file_path.open(mode='wb') as file:
            for chunk in resp.iter_content(chunk_size=4096*4):
                file.write(chunk)
    except Exception as err:
        print(f'{uri}: {err}')


def main():
    dest_dir = Path('downloads/')
    dest_dir.mkdir(exist_ok=True)

    resp = requests.get(BASE_URL)
    doc = soup(resp.content, 'html.parser')
    td_tags = doc.find_all('td', string=re.compile(f'^{TIMESTAMP}'))
    filenames = [
        td.previous_element.string.strip() #.a.string.strip()
        for td in td_tags
    ]
    with ThreadPoolExecutor() as pool:
        for filename in filenames:
            uri = f"{BASE_URL}{filename}"
            file_path = dest_dir / filename
            pool.submit(download_file, uri, file_path)
            print(f'Downloading {filename} ...')

    pd.set_option('display.max_rows', 1000)

    for csv_path in dest_dir.glob('*.csv'):
        df = pd.read_csv(csv_path, low_memory=False)
        df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors='coerce')
        max_temp = df[TARGET_COL].max()
        matched_recs = df[df[TARGET_COL] == max_temp]
        if not len(matched_recs):
            continue
        print()
        print(f'File: {csv_path.name}')
        # lets print the first 5 rows and the first 12 columns
        print(matched_recs.iloc[:5, :12].T)
        print()


if __name__ == "__main__":
    main()
