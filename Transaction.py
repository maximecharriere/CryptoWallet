#!/usr/bin/env python3

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from json import load
from tokenize import Double
from turtle import st
from unittest.main import main
import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import pandas as pd
import numpy as np
import json
import warnings
from tqdm import tqdm

API_KEY = '29d2d18edb47bd2fb5d27ae36e57fe012dd4bd38b8d0b7f65f1de810b0f33f47'

class MyEnum(Enum):
    def __str__(self):
        return self.name

class TransactionType(MyEnum):
    SPOT_TRADE = "Spot trade"
    STAKING_PURCHASE = "Staking purchase"
    STAKING_REDEMPTION = "Staking redemption"
    STAKING_INTEREST = "Staking interest"
    SAVING_PURCHASE = "Saving purchase"
    SAVING_REDEMPTION = "Saving redemption"
    SAVING_INTEREST = "Saving interest"
    DISTRIBUTION = "Distribution"
    DEPOSIT = "Deposit"
    FEE = "Fee"
    WITHDRAW = "Withdraw"
    REFERRAL_INTEREST = "Referral interest"
    MINING_INTEREST = "Mining interest"
    LOST = "Lost"
    STOLEN = "Stolen"
    SPEND = "Spend"
    INCOME = "Income"

class Exchange(MyEnum):
    BINANCE = "Binance"
    SWISSBORG = "Swissborg"
    Coinbase = "Coinbase"


class Wallet(MyEnum):
    SPOT = "Spot"
    SAVING = "Saving"
    STAKING = "Staking"
    
    
@dataclass
class Transaction(object):
    datetime: datetime
    asset: str
    ammount: float
    type: TransactionType
    exchange: Exchange
    userId: str
    wallet: Wallet
    note: str
    price_USD : float = np.NaN
    ammount_USD: float = np.NaN

class Transactions(object):
    data = pd.DataFrame()

    def __init__(self, filename = None):
        super(Transactions, self).__init__()
    
    def load(self, filename):
        self.data = pd.read_csv(filename, parse_dates=['datetime'], converters={
            'type' : lambda s: TransactionType[s],
            'exchange' : lambda s: Exchange[s],
            'wallet' : lambda s: Wallet[s]
        })
        
    def save(self, filename:str):
        self.data.to_csv(filename, index=False)
          
    def append(self, inData: pd.DataFrame):
        newDf = pd.concat([self.data, inData], ignore_index=True)
        self.data = newDf.sort_values("datetime")
        
    def addMissingUsdValue(self):
        tqdm.pandas(desc="Geting USD price")
        print("Remaining missing price: " + str(self.data['price_USD'].isna().sum()))
        self.data.loc[self.data['price_USD'].isna(), "price_USD"] = self.data[self.data['price_USD'].isna()].progress_apply(lambda row: getUsdPrice_nonexcept(row['asset'], row['datetime']), axis=1)
        self.data["ammount_USD"] = self.data["ammount"] * self.data["price_USD"]

BinanceTransactionTypesMap = {
    'Deposit' : TransactionType.DEPOSIT,
    'Withdraw' : TransactionType.WITHDRAW,
    'Buy' : TransactionType.SPOT_TRADE,
    'Sell' : TransactionType.SPOT_TRADE,
    'Transaction Related' : TransactionType.SPOT_TRADE,
    'Small assets exchange BNB' : TransactionType.SPOT_TRADE,
    'Fee' : TransactionType.FEE,
    'Simple Earn Flexible Interest' : TransactionType.SAVING_INTEREST,
    'Simple Earn Flexible Subscription' : TransactionType.SAVING_PURCHASE,
    'Simple Earn Flexible Redemption' : TransactionType.SAVING_REDEMPTION,
    'Simple Earn Locked Rewards' : TransactionType.SAVING_INTEREST,
    'Rewards Distribution' : TransactionType.SAVING_INTEREST,
    'Staking Purchase' : TransactionType.STAKING_PURCHASE,
    'Staking Rewards' : TransactionType.STAKING_INTEREST,
    'Staking Redemption' : TransactionType.STAKING_REDEMPTION,
    'ETH 2.0 Staking' : TransactionType.STAKING_PURCHASE,
    'ETH 2.0 Staking Rewards' : TransactionType.STAKING_INTEREST,
    'Distribution' : TransactionType.DISTRIBUTION,
    'Cash Voucher distribution' : TransactionType.DISTRIBUTION,
    'Commission Fee Shared With You' : TransactionType.REFERRAL_INTEREST,
    'Referral Kickback' : TransactionType.REFERRAL_INTEREST
}


def binanceLoader(filename: str) -> pd.DataFrame:
    inTransactions = pd.read_csv(filename)
    transactions = []
    for idx, row in inTransactions.iterrows():
        transactions.append(Transaction(
            datetime=datetime.fromisoformat(row['UTC_Time']),
            asset=row['Coin'],
            ammount=row['Change'],
            type=BinanceTransactionTypesMap[row['Operation']],
            exchange=Exchange.BINANCE,
            userId=row['User_ID'],
            wallet=Wallet.SPOT, # Default transaction are done with the Spot wallet
            note=row['Operation'] + ('' if row.isna()['Remark']  else (', ' + str(row['Remark'])))
        ))
        
        # Coins in the Saving wallet are prefixed by LD. Put them in the saving wallet and remove the prefix.
        if(transactions[-1].type in {TransactionType.SAVING_PURCHASE, TransactionType.SAVING_REDEMPTION}
           and transactions[-1].asset.startswith('LD')):
            transactions[-1].wallet = Wallet.SAVING
            transactions[-1].note += ', Original asset is ' + transactions[-1].asset
            transactions[-1].asset = transactions[-1].asset[2:]
            
        # The BETH coin is the coin representing ETH coins staked in the ETH 2.0 Staking program.
        # Put them in the Staking wallet and remove the prefix.
        # Reward of this program are given in BETH, so are directly staked and stay in the STAKING wallet.
        if(transactions[-1].asset == 'BETH'):
            transactions[-1].wallet = Wallet.STAKING
            transactions[-1].note += ', Original asset is BETH'
            transactions[-1].asset = 'ETH'
                        
        # In binance the STAKING wallet does not belong to the user. 
        # So during a staking purchase or redemption, there is a transaction telling the in/out flow of the SPOT wallet, 
        # but it does not say the in/out flow of the STAKING wallet. 
        # This transaction is therefore added by this program. 
        # Therefor Binance store the ETH 2.0 Staking in the SPOT wallet with the BETH coin. Don't create a new transaction
        # for ETH 2.0 Staking transactions
        if(transactions[-1].type in {TransactionType.STAKING_PURCHASE, TransactionType.STAKING_REDEMPTION}
           and row['Operation'] != 'ETH 2.0 Staking'):           
            transactions.append(transactions[-1])
            transactions[-1].wallet = Wallet.STAKING
            transactions[-1].ammount *=-1
            transactions[-1].note += ', Transaction not from Binance'
    
    transactions_df = pd.DataFrame(transactions)
    cryptoMap = {
        'IOTA' : 'MIOTA',
        'SHIB2': 'SHIB'
    }
    transactions_df['asset'] = transactions_df['asset'].map(lambda s: cryptoMap[s] if s in cryptoMap else s)
    
    return transactions_df

def getSpotWalletBalance(transactions: pd.DataFrame):
    return transactions[(transactions['wallet'] == Wallet.SPOT)].groupby("asset")['ammount'].sum()

def getSavingWalletBalance(transactions: pd.DataFrame):
    return transactions[(transactions['wallet'] == Wallet.SAVING)].groupby("asset")['ammount'].sum()

def getStakingWalletBalance(transactions: pd.DataFrame):
    return transactions[(transactions['wallet'] == Wallet.STAKING)].groupby("asset")['ammount'].sum()

def getUsdPrice(asset: str, dt: datetime):
    api_url = 'https://min-api.cryptocompare.com/data/v2/histohour'
    api_headers = {
        "authorization": "Apikey " + API_KEY
    }
    api_parameters = {
        'fsym': asset,
        'tsym':'USD',
        'limit':'1',
        'toTs':dt.timestamp(),
        'extraParams':'CryptoWallet'
    } 
    # Call API
    try:
        response = requests.get(api_url, headers=api_headers, params=api_parameters)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        raise
    response_json = json.loads(response.text)
    assert response_json['Response'] == "Success", \
        (f"CryptoCompare API Error : {response_json['Type']} : {response_json['Message']}")
    # Compute hourly mean price
    data = response_json['Data']['Data'][-1]
    mean_price = (data['high'] + data['low'])/2
    return mean_price

def getUsdPrice_nonexcept(asset: str, dt: datetime):
    try:
        return getUsdPrice(asset, dt)
    except Exception as e:
        print(e)
        return np.nan
        

if __name__ == "__main__":
    transactions = Transactions()
    # transactions.append(binanceLoader(r"data/test.csv"))
    # transactions.append(binanceLoader(r"C:\Users\conta\SynologyDrive\Documents\Crypto\Transactions\binance1_2021-03-01 - 2021-10-31.csv"))
    # transactions.append(binanceLoader(r"C:\Users\conta\SynologyDrive\Documents\Crypto\Transactions\binance1_2021-11-01 - 2022-10-20.csv"))
    # transactions.append(binanceLoader(r"C:\Users\conta\SynologyDrive\Documents\Crypto\Transactions\binance2_2021-01-01 - 2021-12-31.csv"))
    # transactions.append(binanceLoader(r"C:\Users\conta\SynologyDrive\Documents\Crypto\Transactions\binance2_2022-01-01 - 2022-10-25.csv"))
    transactions.load("test_exit.csv")
    # transactions.save("test.csv")
    try:
        transactions.addMissingUsdValue()
    finally:
        transactions.save("test_exit2.csv")

