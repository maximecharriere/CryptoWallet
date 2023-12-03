#!/usr/bin/env python3

from .Transaction import TransactionType, WalletType
import pandas as pd
import numpy as np
from tqdm import tqdm
from requests_futures.sessions import FuturesSession
# from dotenv import load_dotenv
# load_dotenv()

def priceComputing_hook(r, *args, **kwargs):
    r_json = r.json()
    if r_json['Response'] != "Success":
        print(f"CryptoCompare API Error : {r_json['Type']} : {r_json['Message']}\n")
        r.price = np.NaN
        return r
    # Compute hourly mean price
    data = r_json['Data']['Data'][-1]
    r.price = (data['high'] + data['low'])/2
    return r

class Wallet(object):
    transactions = pd.DataFrame()
    
    def open(self, filepath_or_buffer):
        self.transactions = pd.read_csv(filepath_or_buffer, parse_dates=['datetime'], converters={
            'type' : lambda s: TransactionType[s],
            'wallet' : lambda s: WalletType[s]
        })
        
    def save(self, filename:str):
        self.transactions.to_csv(filename, index=False)
          
    def addTransactions(self, transactions):
        newDf = pd.concat([self.transactions, transactions], ignore_index=True)
        self.transactions = newDf.sort_values("datetime")
        
    def addMissingUsdValue(self, api_key):
        missingData = self.transactions[self.transactions['price_USD'].isna()]
        print(f"Remaining missing price: {len(missingData)}")
        
        api_url = 'https://min-api.cryptocompare.com/data/v2/histohour'
        with FuturesSession(max_workers=6) as session:
            futures = [session.get(
                url=api_url,
                params={
                    'fsym': asset,
                    'tsym':'USD',
                    'limit':'1',
                    'toTs':datetime.timestamp(),
                    'extraParams':'CryptoWallet',
                    'api_key':api_key
                    },
                hooks = {'response': priceComputing_hook}
                ) for asset, datetime in zip(missingData['asset'], missingData['datetime'])]
            
            prices = [future.result().price for future in tqdm(futures)]
            self.transactions.loc[self.transactions['price_USD'].isna(), "price_USD"] = prices
            self.transactions["ammount_USD"] = self.transactions["ammount"] * self.transactions["price_USD"]
            

    def getSpotWalletBalance(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.SPOT)].groupby("asset")[['ammount', 'ammount_USD']].sum()

    def getSavingWalletBalance(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.SAVING)].groupby("asset")[['ammount', 'ammount_USD']].sum()

    def getStakingWalletBalance(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.STAKING)].groupby("asset")[['ammount', 'ammount_USD']].sum()
        

