from asyncio import (
    set_event_loop_policy, WindowsSelectorEventLoopPolicy,
    get_event_loop, gather, TimeoutError
)
from traceback import format_exception_only
from datetime import datetime, timezone, timedelta
from pathlib import Path
from os import makedirs, remove
from hashlib import sha256

from aiohttp.connector import TCPConnector
from aiohttp.client import ClientSession, ClientError, ClientResponseError

from config import PROXY, USER_AGENT


set_event_loop_policy(WindowsSelectorEventLoopPolicy())
loop = get_event_loop()
client = None
tz = timezone(timedelta(hours=8))


async def try_get(link, error_message):
    for i in range(3):
        try:
            if PROXY != '':
                r = await client.get(link, proxy=PROXY)
            else:
                r = await client.get(link)
            r.raise_for_status()
            return await r.read()
        except ClientResponseError:
            print(f'** E: {error_message}')
            print(f'** E: {r.status}')
            return None
        except (ClientError, TimeoutError) as e:
            print(f'** E{i}: {error_message}')
            print(f'** E{i}: {format_exception_only(e)}')
            if i == 2:
                print('** F')
                raise


async def download_and_check_one(date_str, link_base, name):
    if not await download_one(date_str, link_base, name):
        return False
    while not check_one(date_str, name):
        remove(Path('./data/cache') / name / f'{date_str}.zip')
        remove(Path('./data/cache') / name / f'{date_str}.CHECKSUM')
        if not await download_one(date_str, link_base, name):
            return False
    return True


async def download_one(date_str, link_base, name):
    path_zip = Path('./data/cache') / name / f'{date_str}.zip'
    if not path_zip.exists():
        print(f'download {date_str} zip ...')
        data = await try_get(f'{link_base}{date_str}.zip', date_str)
        if data is None:
            return False
        with open(path_zip, 'wb') as f:
            f.write(data)
    path_checksum = Path('./data/cache') / name / f'{date_str}.CHECKSUM'
    if not path_checksum.exists():
        print(f'download {date_str} CHECKSUM ...')
        data = await try_get(f'{link_base}{date_str}.CHECKSUM', date_str)
        if data is None:
            return False
        with open(path_checksum, 'wb') as f:
            f.write(data)
    print(f'download {date_str} done.')
    return True


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
    today = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    tasks = []
    d = datetime(2020, 6, 1, tzinfo=tz)
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


if __name__ == '__main__':
    loop.run_until_complete(main())
