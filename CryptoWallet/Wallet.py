#!/usr/bin/env python3

import requests
from .Transaction import TransactionType, WalletType
import pandas as pd
import numpy as np
from tqdm import tqdm
from requests_futures.sessions import FuturesSession
import json
import os
import time

# from dotenv import load_dotenv
# load_dotenv()

def responseHandling_hook(r, *args, **kwargs):
    r_json = r.json()
    if r_json['Response'] != "Success":
        print(f"CryptoCompare API Error : {r_json['Type']} : {r_json['Message']}\n")
        r.price = np.NaN
        r.failedReply = r_json
        return r
    # Compute hourly mean price
    data = r_json['Data']['Data'][-1]
    r.price = (data['high'] + data['low'])/2
    r.failedReply = None
    return r

class Wallet(object):
    def __init__(self, apiKey = None, transactionsFilepath = None):
        self.apiKey = apiKey
        if transactionsFilepath is None:
            self.transactions = pd.DataFrame()
        else:
            self.open(transactionsFilepath)
        
    
    def open(self, filepath_or_buffer):
        self.transactions = pd.read_csv(filepath_or_buffer, parse_dates=['datetime'], converters={
            'type' : lambda s: TransactionType[s],
            'wallet' : lambda s: WalletType[s]
        })
        self.printFirstLastTransactionDatetime()
        
    def save(self, filename:str):
        self.transactions.to_csv(filename, index=False)
          
    def addTransactions(self, transactions):
        newDf = pd.concat([self.transactions, transactions], ignore_index=True)
        self.transactions = newDf.sort_values("datetime")
        
    def addMissingUsdValue(self):
        if self.apiKey is None:
            print("No API key provided, no USD values is added")
        missingData = self.transactions[self.transactions['price_USD'].isna()]
        print(f"Remaining missing price: {len(missingData)}")
        
        api_url = 'https://min-api.cryptocompare.com/data/v2/histohour'
        nWorkers = 3
        apiCallLimitPerSecond = 50
        with FuturesSession(max_workers=nWorkers) as session:
            futures = [session.get(
                url=api_url,
                params={
                    'fsym': asset,
                    'tsym':'USD',
                    'limit':'1',
                    'toTs':datetime.timestamp(),
                    'extraParams':'CryptoWallet',
                    'apiKey':self.apiKey
                    },
                hooks = {'response': responseHandling_hook}
                ) for asset, datetime in zip(missingData['asset'], missingData['datetime'])]
            try:
                prices = [future.result().price for future in tqdm(futures)]
                failedReplies = [future.result().failedReply for future in futures]
            except KeyboardInterrupt:
                print("Interrupt received, stopping API requests...")
            finally:#TODO saving in case of interruption is not working
                with open('log/apiFailedReply.json', 'w') as logfile:
                    json.dump(failedReplies, logfile, indent=4)
                self.transactions.loc[self.transactions['price_USD'].isna(), "price_USD"] = prices
                self.transactions["ammount_USD"] = self.transactions["ammount"] * self.transactions["price_USD"]
            

    def getAmmountTot(self):
        return self.transactions.groupby("asset")['ammount'].sum()
    
    def getCostTot(self):
        transactions = self.transactions[(self.transactions['type'].isin([
        TransactionType.SPOT_TRADE, TransactionType.STAKING_PURCHASE, TransactionType.STAKING_REDEMPTION, TransactionType.SAVING_PURCHASE, 
        TransactionType.SAVING_REDEMPTION,TransactionType.DEPOSIT, TransactionType.WITHDRAW, TransactionType.SPEND, TransactionType.INCOME, TransactionType.REDENOMINATION]))]

        return transactions.groupby(["asset"])['ammount_USD'].sum().rename("cost_USD")
    
    def getAssetsList(self):
        return self.transactions['asset'].unique()
    
    def getCurrentPrices(self):
        if self.apiKey is None:
            print("No API key provided, returning empty Series")
            return pd.Series()
        assets = self.getAssetsList()
        api_url = 'https://min-api.cryptocompare.com/data/pricemulti'
        params={
            'fsyms': ','.join(assets),
            'tsyms':'USD',
            'relaxedValidation':'false',
            'extraParams':'CryptoWallet',
            'apiKey':self.apiKey
        }
        response = requests.get(url=api_url,params=params)
        data = response.json()
        if response.status_code != 200:
            print(f"CryptoCompare API Error : {data['Type']} : {data['Message']}\n")
            return pd.Series()
        else:
            # Extract prices into a pandas Series
            prices = {asset: data[asset]['USD'] for asset in assets if asset in data}
            return pd.Series(prices)

    def getCurrentValueTot(self):
        ammount = self.getAmmountTot()
        prices = self.getCurrentPrices()
        value = ammount * prices
        value.name = "current_value_USD"
        return value

    def getPotentialRevenueTot(self):
        value = self.getCurrentValueTot()
        cost = self.getCostTot()
        revenue = value - cost
        revenue.name = "potential_revenue_USD"
        return revenue

    def getFeesTot(self):
        feesTot = self.transactions[(self.transactions['type'] == TransactionType.FEE)].groupby("asset")[['ammount', 'ammount_USD']].sum()
        feesTot.columns = ['fees_ammount', 'fees_USD']
        feesTot['fees_current_USD'] = feesTot['fees_ammount'] * self.getCurrentPrices()
        return feesTot
    
    def getInterestsTot(self):
        interestsTot = self.transactions[(self.transactions['type'].isin([TransactionType.STAKING_INTEREST, TransactionType.SAVING_INTEREST, 
                                                                  TransactionType.REFERRAL_INTEREST, TransactionType.DISTRIBUTION]))].groupby("asset")[['ammount', 'ammount_USD']].sum()
        interestsTot.columns = ['interests_ammount', 'interests_USD']
        interestsTot['interests_current_USD'] = interestsTot['interests_ammount'] * self.getCurrentPrices()
        return interestsTot
    
    
    def getStatsTot(self):
        ammount = self.getAmmountTot()
        cost = self.getCostTot()
        value = self.getCurrentValueTot()
        revenue = self.getPotentialRevenueTot()
        fees = self.getFeesTot()
        interest = self.getInterestsTot()
        stats = pd.concat([ammount, cost, value, revenue, fees, interest], axis=1).fillna(0)
        return stats

    
    def getAmmountSpot(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.SPOT)].groupby("asset")['ammount'].sum()

    def getAmmountSaving(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.SAVING)].groupby("asset")['ammount'].sum()

    def getAmmountStaking(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.STAKING)].groupby("asset")['ammount'].sum()
    
    def printFirstLastTransactionDatetime(self):
        # Group by 'exchange' and aggregate with min and max on 'datetime'
        grouped = self.transactions.groupby('exchange')['datetime'].agg(['min', 'max'])
        # Iterate through the grouped data and print results
        for exchange, row in grouped.iterrows():
            print(f"Exchange: {exchange}")
            print(f"  First Transaction: {row['min']}")
            print(f"  Last Transaction: {row['max']}")
            print()  # Blank line for readability
        
    def exportTradingView(self, filename):
        # Condition 1: Exclude certain assets
        excluded_assets = ['BUSD', 'EUR', 'USD']
        condition1 = ~self.transactions['asset'].isin(excluded_assets)

        # Condition 2: Include only certain transaction types
        included_types = [TransactionType.WITHDRAW, TransactionType.SPOT_TRADE, TransactionType.DEPOSIT]
        condition2 = self.transactions['type'].isin(included_types)

        # Condition 3: Exclude transactions with amount_USD between -10 and 10
        condition3 = ~self.transactions['ammount_USD'].between(-10, 10)

        # Apply all conditions to filter the DataFrame
        transactionsToPlot = self.transactions[condition1 & condition2 & condition3]

        # Write the filtered DataFrame to a txt file
        with open(filename, 'w') as file:
            file.writelines(f"const int numLabels = {str(len(transactionsToPlot))}")
            file.writelines(f"\narray<int> timestamps = array.from({', '.join(transactionsToPlot['datetime'].apply(lambda x: int(x.timestamp()*1000)).astype(str))})")
            file.writelines(f"\narray<float> prices = array.from({', '.join(transactionsToPlot['price_USD'].astype(str))})")
            # file.writelines(f"\narray<bool> buyOrders = array.from({', '.join((transactionsToPlot['ammount']>0).astype(str).str.lower())})")
            file.writelines('\narray<string> assets = array.from({})'.format(', '.join('"{}"'.format(item) for item in transactionsToPlot['asset'])))
            file.writelines(f"\narray<float> ammount = array.from({', '.join(transactionsToPlot['ammount_USD'].astype(str))})")

