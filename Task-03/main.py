from io import BytesIO, TextIOWrapper
import gzip 
from pathlib import Path
import boto3
from botocore.exceptions import ClientError


BUCKET = 'commoncrawl'
KEY = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'


s3 = boto3.client('s3')


def main():
    try:
        print('Downloading the first file')
        with BytesIO() as mem_file:
            s3.download_fileobj(BUCKET, KEY, mem_file)
            data = gzip.decompress(mem_file.getvalue())
    except ClientError as err:
        print(err)
        return

    with (BytesIO(data) as buf,
          TextIOWrapper(buf, encoding='utf-8') as txt):
        key2 = txt.readline().strip()
        print('Final file key:', key2)
        
    final_gz = Path(key2).name
    print('Final file name:', final_gz)

    try:
        print('Downloading the final file')
        with open(final_gz, 'wb') as gz:
            s3.download_fileobj(BUCKET, key2, gz)
    except ClientError as err:
        print(err)
        return

    print('Results:')
    with gzip.open(final_gz, 'rt') as f:
        for line in f:
            print(line)


if __name__ == "__main__":
    main()
