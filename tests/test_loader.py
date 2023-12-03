import pytest 

from CryptoWallet.Wallet import Wallet
from CryptoWallet.Loader import BinanceLoader
import pandas as pd

def test_BinanceUnknowTransactionType():
    with pytest.raises(KeyError):
        BinanceLoader.load("tests/data/test_BinanceUnknowTransactionType.csv")

def test_BinanceNonStdCoins():
    df = BinanceLoader.load("tests/data/test_BinanceNonStdCoins.csv")
    assert df.asset.equals(pd.Series(['MIOTA','SHIB']))
