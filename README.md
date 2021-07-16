## Cryptocurrency price dataset

Cryptocurrency price dataset based on Huobi Global exchange's historical trade data.

## Build

Copy `./config.py.example` into `./config.py`and modify it if needed.

```sh
$ pip install aiohttp
$ python ./download_check_preprocess.py
```

Raw data is stored in `./data/cache/<Name>/`, preprocessed data is generated in `./data/<Name>.npz`.

## Preprocessed data

Preprocessed data is a 1d numpy array which has prices ordered by timestamp. Each value is the price 1 second later than the pervious one. The first price matches timestamp 0 (2020-06-01T00:00:00.000000Z).
