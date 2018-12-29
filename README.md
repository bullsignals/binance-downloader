Binance Downloader
==================

Python tool to download Binance Candlestick (k-line) data from REST API

Instalation
-----------

### LINUX
- clone repository
```console
$ git clone https://github.com/anson-vandoren/binance-downloader.git
$ cd binance-downloader
```
- activate your virtual enviroment and install dependencies (using python >= 3.5).

```console
$ source /path-to-virtual-env/bin/activate
$ pip3 install -r requirements.txt
$ flit install --symlink
```

### WINDOWS

- clone repository
```console
$ git clone https://github.com/anson-vandoren/binance-downloader.git
$ cd binance-downloader
```
- activate your virtual enviroment and install dependencies (using python >= 3.5). 

```console
$ source /path-to-virtual-env/bin/activate
$ pip install -r requirements.txt
``` 

To execute  ```'flit install --symlink'``` you need run command prompt as admin. 
You can do that by:
> Go to start -> All Programs -> Acessories -> Right click on Command Prompt and
> select "Run as administrator"

Then go to project directory, activate you virtual environment e execute the command.
```console
$ flit install --symlink
```
Now you can run the kline-binance command line. You don't need execute the command prompt as admin before do that.

Using the Command Line Interface
-----------------------------

- Help
```console
$  kline-binance --help
usage: kline-binance [-h] [--start START] [--end END] [--output OUTPUT]
                     [--dtfmt DATE_FORMAT]
                     symbol interval

CLI for downloading Binance Candlestick (k-line) data in bulk

positional arguments:
  symbol               (Required) Binance symbol pair, e.g. ETHBTC
  interval             (Required) Frequency interval in minutes(m); hours(h);
                       days(d); weeks(w); months(M); All possibles values: 1m
                       3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M

optional arguments:
  -h, --help           show this help message and exit
  --start START        Start date to get data (inclusive). Format: yyyy/mm/dd
  --end END            End date to get data (exclusive). Format: yyyy/mm/dd
  --output OUTPUT      File name to write data. Default:
                       ./downloaded/timestamp_symbol_interval
  --dtfmt DATE_FORMAT  Format to use for dates (DMY, MDY, YMD, etc). Default:
                       YMD
```

- Downloading data
```console
$ kline-binance -i 1m -l 1500 -st 01/01/2016 -e 05/04/2018
```

License
-------
This code is made available under the MIT License. See LICENSE file for detail.
