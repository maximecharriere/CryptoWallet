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
import pickle

# from dotenv import load_dotenv
# load_dotenv()

class Wallet(object):
    def __init__(self, apiKey = None, databaseFilename = None):
        self.apiKey = apiKey
        self.databaseFilename = databaseFilename
        if self.databaseFilename is not None and os.path.exists(self.databaseFilename):
            self.open(self.databaseFilename)
        else:
            print("No database file provided or file not found, creating an empty wallet.")
            self.transactions = pd.DataFrame()
        
        # Load cache from the pickle file "cache.pkl" if it exists
        self.cacheFilename = "cache.pkl"
        self.cacheLifetime = 5*60 # 5 min in seconds
        if os.path.exists(self.cacheFilename):
          with open(self.cacheFilename, 'rb') as file:
            self.cache = pickle.load(file)
        else:
            self.cache = {}

    def __del__(self):
        self.saveCache()
    
    def open(self, filepath_or_buffer):
        #Check that filepath_or_buffer exists
        if not os.path.exists(filepath_or_buffer):
            raise FileNotFoundError(f"File {filepath_or_buffer} not found")
        self.transactions = pd.read_csv(filepath_or_buffer, parse_dates=['datetime'], date_format='ISO8601', converters={
            'type' : lambda s: TransactionType[s],
            'wallet' : lambda s: WalletType[s]
        })
        self.printFirstLastTransactionDatetime()
        
    def save(self):
        if self.databaseFilename is None:
            raise ValueError("No database filename provided. Set Wallet.databaseFilename.")
        
        self.backup()
            
        # After backup, save the transactions to the original file
        self.transactions.to_csv(self.databaseFilename, index=False)
        print(f"Transactions saved to {self.databaseFilename}")
    
    def backup(self):
        if self.databaseFilename is None:
            raise ValueError("No database filename provided. Set Wallet.databaseFilename.")
        # Backup the original file by doing a copy of it to the "backup/Ymd_HM" folder
        if os.path.exists(self.databaseFilename):
            # Get the directory of the current file and the base filename
            file_dir = os.path.dirname(self.databaseFilename)
            base_filename = os.path.basename(self.databaseFilename)
            
            # Prepare the backup folder path
            backup_folder = os.path.join(file_dir, "backup", time.strftime("%Y%m%d_%H%M"))
            os.makedirs(backup_folder, exist_ok=True)
            
            # Create the backup file path
            backup_path = os.path.join(backup_folder, base_filename)
            
            # Copy the file to the backup location
            # Use shutil.copy2 to preserve metadata, or shutil.copy if metadata is not important
            import shutil
            shutil.copy2(self.databaseFilename, backup_path)
            print(f"Backup saved to {backup_path}")

            
    def saveCache(self):
        with open(self.cacheFilename, 'wb') as file:
            pickle.dump(self.cache, file)
            
    def addTransactions(self, transactions, mergeSimilar = True, removeExisting = True, addMissingUsd = True):
        if mergeSimilar:
            transactions = self.mergeTransactionsInWindow(transactions, window=15*60)
        if removeExisting:
            # Remove transactions that are already in the wallet transactions. For each unique group of "exchange" and "userId", keep the transaction that have a datetime ouside the range between the earliest and latest datetime of the group.
            for (exchange, userId), group in transactions.groupby(['exchange', 'userId']):
                # Test if there is already transactions in the wallet for the same exchange and userId
                if 'exchange' in self.transactions.columns and 'userId' in self.transactions.columns:
                    existingGroupTransactions = self.transactions[(self.transactions['exchange'] == exchange) & (self.transactions['userId'] == userId)]
                    if not existingGroupTransactions.empty:
                        earliest, latest = existingGroupTransactions["datetime"].agg(['min', 'max'])
                        mask = (group['datetime'] >= earliest) & (group['datetime'] <= latest)
                        if mask.any():
                            print(f"Removing {mask.sum()}/{len(group)} transactions from {exchange} {userId} already existing in the wallet.")
                        transactions = transactions.drop(group.index[mask])


        newDf = pd.concat([self.transactions, transactions], ignore_index=True)
        self.transactions = newDf.sort_values("datetime")         
        if addMissingUsd:
            self.addMissingUsdPrice()
                
                        
    def addMissingUsdPrice(self):
        if self.apiKey is None:
            print("No API key provided, no USD values is added")
        self.transactions = self.requestApiMissingUsdPrice(self.transactions, self.apiKey)
        
    def getAmountTot(self):
        return self.transactions.groupby("asset")['amount'].sum()
    
    def getCostTot(self):
        transactions = self.transactions[(self.transactions['type'].isin([
        TransactionType.SPOT_TRADE, TransactionType.STAKING_PURCHASE, TransactionType.STAKING_REDEMPTION, TransactionType.SAVING_PURCHASE, 
        TransactionType.SAVING_REDEMPTION,TransactionType.DEPOSIT, TransactionType.WITHDRAW, TransactionType.SPEND, TransactionType.INCOME, TransactionType.REDENOMINATION]))]

        return transactions.groupby(["asset"])['amount_USD'].sum().rename("cost_USD")
    
    def getAssetsList(self) -> pd.Series:
        return pd.Series(self.transactions['asset'].unique())
          
    def getCurrentPrices(self):
      if self.apiKey is None:
            print("No API key provided, returning empty Series")
            return pd.Series()

      # Get the list of assets
      assets = self.getAssetsList()

      # Check if prices are already cached and not too old
      current_time = time.time()
      cached_assets = {
          asset: price_info for asset, price_info in self.cache.items()
          if current_time - price_info['timestamp'] < 300  # 5 minutes
      }

      # Filter out assets that are not cached or where cache is too old
      assets_to_fetch = [asset for asset in assets if asset not in cached_assets]

      # Fetch prices for assets not in cache or where cache is too old
      if assets_to_fetch:
          new_prices = self.requestApiCurrentPrices(pd.Series(assets_to_fetch), self.apiKey)
          # Update cache with new prices
          for asset, price in new_prices.items():
              self.cache[asset] = {'value': price, 'timestamp': current_time}
          
          self.saveCache()

      # Combine cached prices and new prices
      prices = pd.Series({**{asset: cached_assets[asset]['value'] for asset in cached_assets}, 
                          **{asset: self.cache[asset]['value'] for asset in assets_to_fetch}})

      return prices

    def getCurrentValueTot(self):
        amount = self.getAmountTot()
        prices = self.getCurrentPrices()
        value = amount * prices
        value.name = "current_value_USD"
        return value

    def getPotentialRevenueTot(self):
        value = self.getCurrentValueTot()
        cost = self.getCostTot()
        revenue = value.sub(cost, fill_value=0)
        revenue.name = "potential_revenue_USD"
        return revenue

    def getFeesTot(self):
        feesTot = self.transactions[(self.transactions['type'] == TransactionType.FEE)].groupby("asset")[['amount', 'amount_USD']].sum()
        feesTot.columns = ['fees_amount', 'fees_USD']
        feesTot['fees_current_USD'] = feesTot['fees_amount'] * self.getCurrentPrices()
        return feesTot
    
    def getInterestsTot(self):
        interestsTot = self.transactions[(self.transactions['type'].isin([TransactionType.STAKING_INTEREST, TransactionType.SAVING_INTEREST, 
                                                                  TransactionType.REFERRAL_INTEREST, TransactionType.DISTRIBUTION]))].groupby("asset")[['amount', 'amount_USD']].sum()
        interestsTot.columns = ['interests_amount', 'interests_USD']
        interestsTot['interests_current_USD'] = interestsTot['interests_amount'] * self.getCurrentPrices()
        return interestsTot
    
    
    def getStatsTot(self):
        amount = self.getAmountTot()
        cost = self.getCostTot()
        value = self.getCurrentValueTot()
        revenue = self.getPotentialRevenueTot()
        fees = self.getFeesTot()
        interest = self.getInterestsTot()
        stats = pd.concat([amount, cost, value, revenue, fees, interest], axis=1).fillna(0)
        return stats

    
    def getAmountSpot(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.SPOT)].groupby("asset")['amount'].sum()

    def getAmountSaving(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.SAVING)].groupby("asset")['amount'].sum()

    def getAmountStaking(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.STAKING)].groupby("asset")['amount'].sum()
    
    def printFirstLastTransactionDatetime(self):
        from IPython.display import display
        # Group by 'exchange' and aggregate with min and max on 'datetime'
        grouped = self.transactions.groupby(["exchange", "userId"])['datetime'].agg(earliest=('min'), latest=('max'))
        display(grouped)
    
    def removeTransactionsExchange(self, exchange):
        self.backup()
        if exchange not in self.transactions['exchange'].unique():
            raise ValueError(f"Exchange '{exchange}' not found in the transactions.")
        self.transactions = self.transactions[self.transactions['exchange'] != exchange]
    
    def exportTradingView(self, filename):
        # Condition 1: Exclude certain assets
        excluded_assets = ['BUSD', 'EUR', 'USD', 'USDT', 'FDUSD']
        condition1 = ~self.transactions['asset'].isin(excluded_assets)

        # Condition 2: Include only certain transaction types
        included_types = [TransactionType.SPOT_TRADE]
        condition2 = self.transactions['type'].isin(included_types)

        # Condition 3: Exclude transactions with amount_USD between -10 and 10
        condition3 = ~self.transactions['amount_USD'].between(-10, 10)

        # Apply all conditions to filter the DataFrame
        transactionsToPlot = self.transactions[condition1 & condition2 & condition3]

        # Write the filtered DataFrame to a txt file
        with open(filename, 'w') as file:
            file.writelines(f"const int numLabels = {str(len(transactionsToPlot))}")
            file.writelines(f"\narray<int> timestamps = array.from({', '.join(transactionsToPlot['datetime'].apply(lambda x: int(x.timestamp()*1000)).astype(str))})")
            file.writelines(f"\narray<float> prices = array.from({', '.join(transactionsToPlot['price_USD'].astype(str))})")
            # file.writelines(f"\narray<bool> buyOrders = array.from({', '.join((transactionsToPlot['amount']>0).astype(str).str.lower())})")
            file.writelines('\narray<string> assets = array.from({})'.format(', '.join('"{}"'.format(item) for item in transactionsToPlot['asset'])))
            file.writelines(f"\narray<float> amount = array.from({', '.join(transactionsToPlot['amount_USD'].astype(str))})")

    @staticmethod
    def mergeTransactionsInWindow(transactions, window): # window in seconds
        # Group transactions based on unique combination of attributes
        grouped_transactions = transactions.groupby(['asset', 'type', 'exchange', 'userId', 'wallet', 'note'], sort=False)

        # Define a function to merge transactions within a 15-minute window
        def merge_transactions(transaction_group):
            time_diff = transaction_group['datetime'].diff().dt.total_seconds()
            mask = (time_diff > window) | time_diff.isnull()
            groups = mask.cumsum().rename('group')
            agg_dict = {
                'amount': 'sum',
                'price_USD': 'mean',
                'amount_USD': 'sum',
                'datetime': 'first'
            }
            return transaction_group.groupby(groups, sort=False, group_keys=False).agg(agg_dict)

        # Apply the function to each group and concatenate the results
        merged_transactions = grouped_transactions.apply(merge_transactions)
        merged_transactions.reset_index(inplace=True)
        merged_transactions.drop(columns='group', inplace=True)
        return merged_transactions
      
    @staticmethod
    def requestApiCurrentPrices(assets, apiKey) -> pd.Series:
        # Rename assets to match CryptoCompare API
        assets = assets.replace(Wallet.CryptoCompareAssetMap)

        # Create batch of assets where the joinded length is less than 300 characters
        MAX_FSYMS_LENGH = 300
        assets_batches = []
        assets_batch = []
        for asset in assets:
            batch_length = len(','.join(assets_batch + [asset]))

            # If the batch is too long, append the current batch to the list of batches and start a new batch
            if batch_length > MAX_FSYMS_LENGH:
              assets_batches.append(assets_batch.copy())
              assets_batch.clear()              
              
            assets_batch.append(asset)
          
        # Append the last batch to the list of batches
        assets_batches.append(assets_batch) 
              
        # Request prices for each batch of assets to th API
        prices = []
        for batch in assets_batches:
            batch_prices = Wallet.requestApiCurrentPricesBatch(batch, apiKey)
            prices.append(batch_prices)
        prices = pd.concat(prices)
        
        # Replace back the original asset names using the CryptoCompareAssetMap
        cryptoCompareAssetMap_inverted = {v: k for k, v in Wallet.CryptoCompareAssetMap.items()}
        prices = prices.rename(index=cryptoCompareAssetMap_inverted)
        
        return prices
              
    @staticmethod
    def requestApiCurrentPricesBatch(assetsBatch, apiKey):
        api_url = 'https://min-api.cryptocompare.com/data/pricemulti'
        params={
            'fsyms': ','.join(assetsBatch),
            'tsyms':'USD',
            'relaxedValidation':'false',
            'extraParams':'CryptoWallet',
            'apiKey': apiKey
        }
        try:
            response = requests.get(url=api_url,params=params)
        except requests.exceptions.RequestException as e:
            print(f"Request Error to CryptoCompare API : {e}\n")
            return pd.Series()
        if response.status_code != 200:
            print(f"Request Error to CryptoCompare API : {response.status_code}")
            return pd.Series()
        
        data = response.json()
        if 'Response' in data.keys():
            print(f"CryptoCompare API Error : {data['Message']}\n")
            return pd.Series()
        # Extract prices into a pandas Series
        prices = pd.Series({asset: data[asset]['USD'] for asset in assetsBatch if asset in data})

        return prices


    @staticmethod
    def requestApiMissingUsdPrice(transactions, apiKey):
        # Select transactions with missing usd price
        missingUsdPrice = transactions[transactions['price_USD'].isna()].copy()  # Copy to avoid SettingWithCopyWarning
        # remove the unsuported assets by the api
        missingUsdPrice = missingUsdPrice[~missingUsdPrice['asset'].isin(Wallet.CryptoCompareUnsupportedAssets)]
        
        print(f"Remaining missing price to request: {len(missingUsdPrice)}")
        
        if missingUsdPrice.empty:
            return transactions

        # Rename assets to match CryptoCompare API
        missingUsdPrice['asset'] = missingUsdPrice['asset'].replace(Wallet.CryptoCompareAssetMap)
        
        # API setup
        api_url = 'https://min-api.cryptocompare.com/data/v2/histohour'
        nWorkers = 3
        # apiCallLimitPerSecond = 50
        
        with FuturesSession(max_workers=nWorkers) as session:
            futures = []
             # Submit requests with row indices as metadata
            for idx, row in missingUsdPrice.iterrows():
                future = session.get(
                    url=api_url,
                    params={
                        'fsym': row['asset'],
                        'tsym':'USD',
                        'limit':'1',
                        'toTs':row['datetime'].timestamp(),
                        'extraParams':'CryptoWallet',
                        'apiKey':apiKey
                    }
                )
                # Attach the index to each future so we know where to place the result
                future.idx = idx
                futures.append(future)
            
            prices = {}
            failedReplies = {}
            
            # Process each completed future
            try:
                for future in tqdm(futures, desc="Fetching prices"):
                    response = future.result()
                    
                    # Check if API request was successful
                    if response.status_code != 200:
                        print(f"Request Error: Status {response.status_code}")
                        failedReplies[future.idx] = {"status_code": response.status_code}
                        continue
                        
                    json_data = response.json()
                    # Check if API reponse is not successful
                    if json_data['Response'] != "Success":
                        print(f"API Error {json_data['Type']}: {json_data['Message']}")
                        failedReplies[future.idx] = json_data
                        continue
                    
                    # Extract and calculate the average price
                    try:
                        data = json_data['Data']['Data'][-1]
                        avg_price = (data['high'] + data['low']) / 2
                        prices[future.idx] = avg_price
                    except (KeyError, IndexError, TypeError) as e:
                        print(f"Data parsing error for index {future.idx}: {e}")
                        failedReplies[future.idx] = {"error": str(e)}
        
            except KeyboardInterrupt: # Stop API request, but don't propagate the exception, to continue the rest of the code
                print("Interrupt received, stopping API requests...")
                
            finally:
                # Save failed replies to a log file if there are any
                if failedReplies:
                    os.makedirs('log', exist_ok=True)
                    with open('log/apiFailedReply.json', 'w') as logfile:
                        json.dump(failedReplies, logfile, indent=4)
                
                # Update the transactions DataFrame with prices
                price_series = pd.Series(prices)
                transactions.loc[price_series.index, 'price_USD'] = price_series
                transactions.loc[price_series.index, 'amount_USD'] = (
                    transactions.loc[price_series.index, 'amount'] * transactions.loc[price_series.index, 'price_USD']
                )
        
        return transactions
    
    CryptoCompareAssetMap = {
        'IOTA': 'MIOTA',
        'STRK': 'STARK',
        'MNT': 'MANTLE'
    }  
    CryptoCompareUnsupportedAssets = ['1000PEPPER']
    
    
    
