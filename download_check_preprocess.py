from asyncio import (
    set_event_loop_policy, WindowsSelectorEventLoopPolicy,
    get_event_loop, gather, TimeoutError
)
from traceback import format_exception_only
from datetime import datetime, timezone, timedelta
from pathlib import Path
from os import makedirs, remove
from hashlib import sha256
from zipfile import ZipFile
from csv import DictReader
from io import StringIO

from aiohttp.connector import TCPConnector
from aiohttp.client import ClientSession, ClientError
from numpy import empty, float64, savez_compressed

from config import PROXY, USER_AGENT


set_event_loop_policy(WindowsSelectorEventLoopPolicy())
loop = get_event_loop()
client = None

async def try_get(link, error_message):
    for i in range(3):
        try:
            if PROXY != '':
                return await (await client.get(
                    link, proxy=PROXY
                )).read()
            else:
                return await (await client.get(
                    link
                )).read()
        except (ClientError, TimeoutError) as e:
            print(f'** E{i}: {error_message}')
            print(f'** E{i}: {format_exception_only(e)}')
            if i == 2:
                print('** F')
                raise

async def download_and_check_one(date_str, link_base, name):
    await download_one(date_str, link_base, name)
    while not check_one(date_str, name):
        remove(Path('./data/cache') / name / f'{date_str}.zip')
        remove(Path('./data/cache') / name / f'{date_str}.CHECKSUM')
        await download_one(date_str, link_base, name)

async def download_one(date_str, link_base, name):
    path_zip = Path('./data/cache') / name / f'{date_str}.zip'
    if not path_zip.exists():
        print(f'download {date_str} zip ...')
        data = await try_get(f'{link_base}{date_str}.zip', date_str)
        with open(path_zip, 'wb') as f:
            f.write(data)
    path_checksum = Path('./data/cache') / name / f'{date_str}.CHECKSUM'
    if not path_checksum.exists():
        print(f'download {date_str} CHECKSUM ...')
        data = await try_get(f'{link_base}{date_str}.CHECKSUM', date_str)
        with open(path_checksum, 'wb') as f:
            f.write(data)
    print(f'download {date_str} done.')

def check_one(date_str, name):
    print(f'check {date_str} ...')
    with open(Path('./data/cache') / name / f'{date_str}.zip', 'rb') as f:
        data = f.read()
    hash_data = sha256()
    hash_data.update(data)
    hash_data = hash_data.hexdigest()
    with open(Path('./data/cache') / name / f'{date_str}.CHECKSUM', 'r') as f:
        checksum = f.read(64)
    if hash_data == checksum:
        print(f'check {date_str} done.')
        return True
    else:
        print(f'** E: check {date_str} failed.')
        return False

def get_rows_one_file(date_str, name):
    with ZipFile(Path('./data/cache') / name / f'{date_str}.zip', 'r') as f:
        data = f.read(f'{name}-trades-{date_str}.csv')
    return iter(DictReader(
        StringIO(data.decode('ascii')),
        ('id', 'ts', 'price', 'amount', 'direction')
    ))

def get_rows(name):
    one_day = timedelta(days=1)
    today = (
        datetime.now(timezone.utc) - one_day - one_day
    ).replace(hour=0, minute=0, second=0, microsecond=0)
    d = datetime(2020, 6, 1, tzinfo=timezone.utc)
    while d <= today:
        date_str = d.strftime('%Y-%m-%d')
        print(f'preprocess {date_str} ...')
        yield from get_rows_one_file(date_str, name)
        print(f'preprocess {date_str} done.')
        d += one_day

def preprocess(name):
    rows = get_rows(name)
    row = next(rows)
    one_day = timedelta(days=1)
    t = int(datetime(2020, 6, 1, tzinfo=timezone.utc).timestamp())
    now = int(
        (
            datetime.now(timezone.utc) - one_day
        ).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    )
    data_t = timedelta(milliseconds=int(row['ts'])).total_seconds()
    latest_price = row['price']
    current_price = row['price']
    skip_flag = False
    while t < now:
        if not skip_flag:
            try:
                while data_t < t:
                    row = next(rows)
                    data_t = timedelta(milliseconds=int(row['ts'])).total_seconds()
                    latest_price = current_price
                    current_price = row['price']
            except StopIteration:
                skip_flag = True
                latest_price = current_price
        yield float64(latest_price)
        t += 1

async def main():
    name = input('name = ')

    print('download and check ...')
    global client
    client = ClientSession(
        connector=TCPConnector(limit=20),
        loop=loop,
        headers={
            'user-agent': USER_AGENT
        }
    )
    makedirs(Path('./data/cache') / name, exist_ok=True)
    one_day = timedelta(days=1)
    today = (
        datetime.now(timezone.utc) - one_day - one_day
    ).replace(hour=0, minute=0, second=0, microsecond=0)
    tasks = []
    d = datetime(2020, 6, 1, tzinfo=timezone.utc)
    while d <= today:
        date_str = d.strftime('%Y-%m-%d')
        link_base = (
            f'https://futures.huobi.com/data/trades/spot/daily/{name}/'
            f'{name}-trades-'
        )
        tasks.append(loop.create_task(download_and_check_one(
            date_str, link_base, name
        )))
        d += one_day
    await gather(*tasks)
    await client.close()
    print('download and check done.')

    print('preprocess ...')
    length = int(
        (datetime.now(timezone.utc) - one_day)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .timestamp()
    ) - int(datetime(2020, 6, 1, tzinfo=timezone.utc).timestamp())
    arr = empty((length,), dtype=float64)
    for i, v in enumerate(preprocess(name)):
        arr[i] = v
    print('save compressed ...')
    savez_compressed(Path('./data') / f'{name}', arr)
    print('preprocess done.')

    print('done.')


if __name__ == '__main__':
    loop.run_until_complete(main())