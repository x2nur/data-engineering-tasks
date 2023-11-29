import csv
import json
from pathlib import Path


SRC_DIR = 'data'


def flatten(obj, name=''):
    res = {}
    if isinstance(obj, dict):
        for key, val in obj.items():
            res |= flatten(val, f'{name}{key}_')
        return res
    if isinstance(obj, list):
        for key, val in enumerate(obj):
            res |= flatten(val, f'{name}{key}_')
        return res
    name = name.rstrip('_')
    res[name] = obj
    return res


def main():
    src = Path(SRC_DIR)
    for path in src.rglob('*.json'):
        with path.open(mode='r') as json_f:
            doc = json.load(json_f)
            doc = flatten(doc)
            with open(f'{path.stem}.csv', 'w') as csv_f:
                wr = csv.DictWriter(csv_f, fieldnames=doc.keys())
                wr.writeheader()
                wr.writerow(doc)


if __name__ == "__main__":
    main()
