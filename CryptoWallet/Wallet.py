#!/usr/bin/env python3

from .Transaction import TransactionType, WalletType
import pandas as pd
import numpy as np
import os
import time
import pickle
from .CryptoCompareWrapper import CryptoCompareWrapper

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
        self.cacheFilename = ".CryptoWallet/currentPriceCache.pkl"
            
        self.cacheLifetime = 60*60 # 60 min in seconds
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
            'wallet' : lambda s: WalletType[s],
            'userId' : lambda s: str(s)
        })
        self.printFirstLastTransactionDatetime()
        
    def save(self):
        if self.databaseFilename is None:
            raise ValueError("No database filename provided. Set Wallet.databaseFilename.")
        
        # Add missing USD price if needed
        self.addUsdData()
        
        self.checkIntegrity()
        
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

    def checkIntegrity(self):        
        # Check that datetime, asset, amount, type, exchange, userId, wallet are not null
        required_columns = ['datetime', 'asset', 'amount', 'type', 'exchange', 'userId', 'wallet']
        if self.transactions[required_columns].isnull().values.any():
            raise ValueError("Integrity check failed: NaN values found in required columns.")
        
        # Check that their is no transaction type still set to TransactionType.TBD
        if (self.transactions['type'] == TransactionType.TBD).any():
            raise ValueError("Integrity check failed: Transaction type TBD found in some transactions.")
        
        # Check that amount_USD = amount * price_USD, with a tolerance of 0.001
        amount_USD = self.transactions['amount'] * self.transactions['price_USD']
        equal = np.isclose(amount_USD, self.transactions['amount_USD'], rtol=0, atol=0.001, equal_nan=True)
        if not equal.all():
            raise ValueError("Integrity check failed: amount_USD != amount * price_USD\n" + str(self.transactions[~equal]))
        
        
    def saveCache(self):
        # create the directory to store the cache
        directory = os.path.dirname(self.cacheFilename)
        if directory:  # Only create directories if there's actually a path component
            os.makedirs(directory, exist_ok=True)
            
        with open(self.cacheFilename, 'wb') as file:
            pickle.dump(self.cache, file)
            
    def addTransactions(self, transactions, mergeSimilar = True, removeExisting = True):
        if transactions.empty:
            return
        # Change crypto names to match the standard names
        transactions['asset'] = transactions['asset'].map(lambda s: Wallet.CryptoNameMap[s] if s in Wallet.CryptoNameMap else s)
                
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
                    
    def getAmountTotByAsset(self):
        return self.transactions.groupby("asset")['amount'].sum()
            
    
    def getCostTot(self):
        transactions = self.transactions[(self.transactions['type'].isin([
        TransactionType.SPOT_TRADE, TransactionType.STAKING_PURCHASE, TransactionType.STAKING_REDEMPTION, TransactionType.SAVING_PURCHASE, 
        TransactionType.SAVING_REDEMPTION,TransactionType.DEPOSIT, TransactionType.WITHDRAW, TransactionType.SPEND, TransactionType.INCOME, TransactionType.REDENOMINATION, TransactionType.ACCOUNT_TRANSFER]))]

        return transactions.groupby(["asset"])['amount_USD'].sum().rename("cost_USD")
    
    def getTransactions(self, remove_datetime_timezone = False):
        transactions = self.transactions.copy()
        if remove_datetime_timezone:
            transactions['datetime'] = transactions['datetime'].dt.tz_localize(None)
        return transactions
        
    
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
      cached_assets = [asset for asset, price_info in self.cache.items() if (current_time - price_info['timestamp']) < self.cacheLifetime] 

      # Get assets that are not in cached_assets
      assets_to_fetch = list(set(assets) - set(cached_assets))

      # Fetch prices for assets not in cache or where cache is too old
      if assets_to_fetch:
          new_prices = CryptoCompareWrapper.requestApiCurrentPrices(pd.Series(assets_to_fetch), self.apiKey)
          # Update cache with new prices
          for asset, price in new_prices.items():
              self.cache[asset] = {'value': price, 'timestamp': current_time}
          
          self.saveCache()

      # return the prices of the assets in "assets" as a Series, taking the value from the self.cache[asset]['value']. If the asset is not in the cache, enter np.nan as the value.
      prices = pd.Series({asset: self.cache.get(asset, {'value': np.nan})['value'] for asset in assets})

      return prices

    def getCurrentValueTot(self):
        amount = self.getAmountTotByAsset()
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

    def getBuyPriceTot(self):
        amount = self.getAmountTotByAsset()
        cost = self.getCostTot()
        # replace the amount of less than 0.0001 to NaN to avoid division by zero, negative and huge results due to really small amounts
        amount = amount.where(amount >= 0.0001)
        buyPrice = cost / amount
        # remove negative buy prices (when profit is already realized)
        buyPrice = buyPrice.where(buyPrice >= 0)
        buyPrice.name = "buy_price_USD"
        return buyPrice
        
        
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
    
    
    def getCoinsStats(self):
        amount = self.getAmountTotByAsset()
        cost = self.getCostTot()
        value = self.getCurrentValueTot()
        revenue = self.getPotentialRevenueTot()
        buyPrice = self.getBuyPriceTot()
        fees = self.getFeesTot()
        interest = self.getInterestsTot()
        stats = pd.concat([amount, cost, value, revenue, buyPrice, fees, interest], axis=1).fillna(0)
        return stats

    def getSummary(self):
        total_holding_USD = self.getCurrentValueTot().drop(self.Fiats).sum()
        total_fiat_expenses_USD = self.getCurrentValueTot().loc[self.Fiats].sum()
        profit_USD = total_holding_USD + total_fiat_expenses_USD
        total_fees_USD = self.getFeesTot()['fees_USD'].sum()
        total_interests_USD = self.getInterestsTot()['interests_USD'].sum()
            
        return pd.Series({
            'total_holding_USD': total_holding_USD,
            'total_fiat_expenses_USD': total_fiat_expenses_USD,
            'profit_USD': profit_USD,
            'total_fees_USD': total_fees_USD,
            'total_interests_USD': total_interests_USD
        })
    
    def getAmountSpot(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.SPOT)].groupby("asset")['amount'].sum()

    def getAmountSaving(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.SAVING)].groupby("asset")['amount'].sum()

    def getAmountStaking(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.STAKING)].groupby("asset")['amount'].sum()
    
    def getAmountFunding(self):
        return self.transactions[(self.transactions['wallet'] == WalletType.FUNDING)].groupby("asset")['amount'].sum()
        
    def get_historical_amount(self, asset: str) -> pd.DataFrame:
        asset_txs = self.transactions[self.transactions['asset'] == asset].copy()
        
        if asset_txs.empty:
            return pd.DataFrame(columns=['timestamp', 'amount'])
        
        asset_txs = asset_txs.sort_values('datetime')
        
        daily_amounts = (
            asset_txs
            .set_index('datetime')
            ['amount']
            .resample('D')
            .sum()
            .fillna(0)
        )
        
        cumulative = daily_amounts.cumsum()
        
        return pd.DataFrame({
            'timestamp': cumulative.index,
            'amount': cumulative.values
        })
    
            
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
        
    def getWalletsBalance(self):
        prices = self.getCurrentPrices()
        amount_df = self.transactions.groupby(['exchange', 'asset'])['amount'].sum().unstack(level=0, fill_value=0)
        # add columns for each exchange with the current value in USD
        for exchange in amount_df.columns:
            amount_df[f"{exchange}_USD"] = amount_df[exchange] * prices
        # sort the columns by exchange name
        amount_df = amount_df.reindex(sorted(amount_df.columns), axis=1)
        return amount_df
    
    def exportTradingView(self, filename):
        # TODO. Rename S to FTM, POL to MATIC
        ## 1st Script : Buy Data ##
        # Get the list of buy prices. Set nan values to 0 to avoid errors in the script
        buy_prices = self.getBuyPriceTot().fillna(0)
        # Get the total amount of each asset
        amount = self.getAmountTotByAsset().fillna(0)
        
        ## 2nd Script : Buy/Sell Orders ## 
        # Condition 1: Exclude certain assets
        excluded_assets = ['BUSD', 'EUR', 'USD', 'USDT', 'FDUSD', 'CHF']
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
            file.writelines("// #### 1st Script : Assets Data ####")
            file.writelines(f"\nconst int assets_count = {str(len(buy_prices))}")
            file.writelines(f"\narray<string> assets = array.from({', '.join(f'"{item}"' for item in buy_prices.index)})")
            file.writelines(f"\narray<float> assets_buyPrice = array.from({', '.join(buy_prices.astype(str))})")
            file.writelines(f"\narray<float> assets_amount = array.from({', '.join(amount.astype(str))})")
            
            file.writelines("\n\n// #### 2nd Script : Buy/Sell Orders ####")
            file.writelines(f"\nconst int orders_count = {str(len(transactionsToPlot))}")
            file.writelines(f"\narray<int> orders_timestamp = array.from({', '.join(transactionsToPlot['datetime'].apply(lambda x: int(x.timestamp()*1000)).astype(str))})")
            file.writelines(f"\narray<float> orders_price = array.from({', '.join(transactionsToPlot['price_USD'].astype(str))})")
            file.writelines(f"\narray<string> orders_asset = array.from({', '.join(f'"{item}"' for item in transactionsToPlot['asset'])})")
            file.writelines(f"\narray<float> orders_amount_USD = array.from({', '.join(transactionsToPlot['amount_USD'].astype(str))})")
           




    @staticmethod
    def mergeTransactionsInWindow(transactions, window): # window in seconds
        # Group transactions based on unique combination of attributes
        grouped_transactions = transactions.groupby(['asset', 'type', 'exchange', 'userId', 'wallet', 'note'], sort=False)

        # Define a function to merge transactions within a 15-minute window
        def merge_transactions(transaction_group):
            time_diff = transaction_group['datetime'].diff().dt.total_seconds()
            mask = (abs(time_diff) > window) | time_diff.isnull()
            groups = mask.cumsum().rename('group')
            agg_dict = {
                'amount': lambda x: x.sum(skipna=False),
                'price_USD': lambda x: np.average(x, weights=transaction_group.loc[x.index, 'amount']),
                'amount_USD': lambda x: x.sum(skipna=False),
                'datetime': 'first'
            }
            return transaction_group.groupby(groups, sort=False, group_keys=False).agg(agg_dict)

        # Apply the function to each group and concatenate the results
        merged_transactions = grouped_transactions.apply(merge_transactions)
        merged_transactions.reset_index(inplace=True)
        merged_transactions.drop(columns='group', inplace=True)
        return merged_transactions
      
    def addUsdData(self):
        self.transactions = CryptoCompareWrapper.addMissingUsdPrice(self.transactions, self.apiKey)
        self.transactions = self.addMissingUsdAmount(self.transactions)

    @staticmethod
    def addMissingUsdAmount(transactions):
        missingUsdAmount = transactions['amount_USD'].isna()
        transactions.loc[missingUsdAmount, 'amount_USD'] = (transactions.loc[missingUsdAmount, 'amount'] * transactions.loc[missingUsdAmount, 'price_USD'])
        return transactions
    

    
    CryptoNameMap = {
        'SHIB2': 'SHIB',
        'BEAMX':'BEAM',
        'BTCB': 'BTC',
        'DASHA': 'VVAIFU'
    }
    Fiats = ['USD', 'EUR', 'CHF']



