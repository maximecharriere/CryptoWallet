try:
  import google.colab
  from google.colab import files
  IN_COLAB = True
except:
  IN_COLAB = False
import os
import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import io
import pandas as pd
import numpy as np
DEV_MODE = False

from CryptoWallet.Wallet import Wallet
from CryptoWallet.Loader import BinanceLoader


wallet = Wallet()

if IN_COLAB:
    files = files.upload()
else:
    files = [r"C:\Users\conta\SynologyDrive\Documents\Crypto\Transactions\binance1_2021-03-01 - 2021-10-31.csv", 
             r"C:\Users\conta\SynologyDrive\Documents\Crypto\Transactions\binance1_2021-11-01 - 2022-10-20.csv",
             r"C:\Users\conta\SynologyDrive\Documents\Crypto\Transactions\binance2_2021-01-01 - 2021-12-31.csv",
             r"C:\Users\conta\SynologyDrive\Documents\Crypto\Transactions\binance2_2022-01-01 - 2022-10-25.csv"]

for file in files:
    wallet.addTransactions(BinanceLoader.load(file))
    
wallet.save("data/transactions.csv")