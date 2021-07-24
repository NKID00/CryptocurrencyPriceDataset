from datetime import datetime, timezone, timedelta
from pathlib import Path
from zipfile import ZipFile
from csv import DictReader
from io import StringIO

from numpy import empty, float64, savez_compressed


tz = timezone(timedelta(hours=8))


def get_rows(files, name):
    for path in files:
        print(f'preprocess {path.stem} ...')
        with ZipFile(path, 'r') as f:
            data = f.read(f'{name}-trades-{path.stem}.csv')
        yield from iter(DictReader(
            StringIO(data.decode('ascii')),
            ('id', 'ts', 'price', 'amount', 'direction')
        ))


def main():
    name = input('name = ')

    print('preprocess ...')
    files = Path(f'./data/cache/{name}').iterdir()
    files = sorted(filter(lambda path: path.suffix == '.zip', files))
    t = datetime.strptime(files[0].stem, '%Y-%m-%d').replace(tzinfo=tz)
    t = int(t.timestamp())
    latest = datetime.strptime(files[-1].stem, '%Y-%m-%d').replace(tzinfo=tz)
    latest += timedelta(days=1)
    latest = int(latest.timestamp())

    arr = empty((latest - t,), dtype=float64)

    rows = get_rows(files, name)
    row = next(rows)
    data_t = timedelta(milliseconds=int(row['ts'])).total_seconds()
    latest_price = row['price']
    current_price = row['price']
    skip_flag = False
    i = 0

    while t < latest:
        if not skip_flag:
            try:
                while data_t < t:
                    row = next(rows)
                    data_t = (
                        timedelta(milliseconds=int(row['ts'])).total_seconds()
                    )
                    latest_price = current_price
                    current_price = row['price']
            except StopIteration:
                skip_flag = True
                latest_price = current_price
        arr[i] = float64(latest_price)
        t += 1
        i += 1

    print('save compressed ...')
    savez_compressed(Path('./data') / f'{name}', arr)

    print('preprocess done.')

if __name__ == '__main__':
    main()
