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
$ binance -h 
```

- Downloading data
```console
$ binance -i 1m -l 1500 -st 01/01/2016 -e 05/04/2018
```

License
-------
This code is under MIT License. See LICENSE file for detail.