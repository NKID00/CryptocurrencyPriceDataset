## CryptocurrencyPriceDataset

Cryptocurrency price dataset based on [Huobi Global exchange's historical trade data](https://github.com/hbdmapi/huobi_public_data).

It is possible to download the preprocessed dataset from [the release page](https://github.com/NKID00/CryptocurrencyPriceDataset/releases), but it may not contain the latest data.

## Build the dataset

1. Install the dependences:

   ```sh
   $ pip install aiohttp numpy
   ```

2. Copy `./config.py.example` into `./config.py` and modify it if needed.

3. Run this script to download the data and validate the checksum:

   ```sh
   $ python ./download_check.py
   ```

   Raw data is stored in `./data/cache/<Name>/`.

4. Run this script to preprocess the data:

   ```sh
   $ python ./preprocess
   ```

   Preprocessed dataset is generated in `./data/<Name>.npz`.

## Preprocessed dataset

Preprocessed dataset file contains a 1d NumPy array `arr_0` which has prices ordered by timestamp. Each value is the price 1 second later than the pervious one. The first price matches timestamp 0 (2020-06-01T00:00:00.000000Z).
