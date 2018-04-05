Binance Downloader
==================

Python tool to download Binance Candlestick (k-line) data from REST API


Instalation
-----------
- clone repository
```console
$ git clone https://github.com/bullsignals/binance-downloader.git
$ cd binance-downloader
```
- activate your virtual enviroment and install dependencies.(using python >= 3.5)

```console
$ source /path-to-virtual-env/bin/activate
$ pip install -r requirements.txt
$ flit install --symlink --python python
```
Using binance downloader cli
-----------------------------

- Help
```console
$ kline-binance -h

usage: binance [-h] --interval INTERVAL [--symbol SYMBOL] [--limit LIMIT]
               [--start START] [--end END] [--output OUTPUT]

Python tool to download Binance Candlestick (k-line) data from REST API

optional arguments:
  -h, --help            show this help message and exit
  --interval INTERVAL, -i INTERVAL
                        frequence interval in minutes(m); hours(h); days(d);
                        weeks(w); months(M); all possibles values: 1m 3m 5m
                        15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M
  --symbol SYMBOL, -s SYMBOL
                        pair. default: 'ETHBTC'.
  --limit LIMIT, -l LIMIT
                        quantity of items downloaded;
  --start START, -st START
                        start period to get data. format: dd/mm/yy
  --end END, -e END     start period to get data. format: dd/mm/yy
  --output OUTPUT, -o OUTPUT
                        File Name. default: binance. 
```

- Downloading data
```console
$ kline-binance -i 1m -l 1500 -st 01/01/2016 -e 05/04/2018
```

License
-------
This code is under MIT License. See LICENSE file for detail.