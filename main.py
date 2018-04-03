from binance.client import BinanceAPI
from binance.db import to_csv

def main():
    binance = BinanceAPI(interval='12h')
    binance.consult()
    print(binance.klines)
    to_csv(binance.klines)

if __name__ == '__main__':
    main()