import pandas as pd
import numpy as np
from requests_futures.sessions import FuturesSession
from tqdm import tqdm
import time
import os
import json
import requests

class CryptoCompareWrapper():
    def __init__(self):
        pass
    
    AssetNameMap = {
        'IOTA': 'MIOTA',
        'MNT': 'MANTLE'
    }  
    UnsupportedHistoricalPriceAssets = ['1000PEPPER', 'CHILLGUY', 'UOS', 'HYPE', 'SDM', 'GNET', 'XBG', 'BIO', 'PAWSY', 'WGC']
    UnsupportedCurrentPriceAssets = ['1000PEPPER', 'GNET', 'XBG', 'BIO', 'PAWSY', 'WGC']
    
    @staticmethod
    def requestApiCurrentPrices(assets :pd.Series, apiKey) -> pd.Series:
        # Rename assets to match CryptoCompare API
        assets = assets.replace(CryptoCompareWrapper.AssetNameMap)
        
        # remove the unsuported assets by the api
        assets = assets[~assets.isin(CryptoCompareWrapper.UnsupportedCurrentPriceAssets)]
        
        if assets.empty:
            return pd.Series()

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
            batch_prices = CryptoCompareWrapper.__requestApiCurrentPricesBatch(batch, apiKey)
            prices.append(batch_prices)
        prices = pd.concat(prices).squeeze()
        
        # Replace back the original asset names using the AssetNameMap
        AssetNameMap_inverted = {v: k for k, v in CryptoCompareWrapper.AssetNameMap.items()}
        prices = prices.rename(index=AssetNameMap_inverted)
        
        return prices
              
    @staticmethod
    def __requestApiCurrentPricesBatch(assetsBatch, apiKey):
        api_url = 'https://min-api.cryptocompare.com/data/pricemulti'
        params={
            'fsyms': ','.join(assetsBatch),
            'tsyms':'USD',
            'relaxedValidation':'true',
            'extraParams':'CryptoWallet',
            'apiKey': apiKey
        }
        try:
            response = requests.get(url=api_url,params=params)
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request Error to CryptoCompare API : {e}\n")
        if response.status_code != 200:
            raise Exception(f"Request Error to CryptoCompare API : {response.status_code}")
        
        data = response.json()
        if 'Response' in data.keys():
            if data['Response'] != "Error": # If the API return a "Response" key, but it's not "Error", then it's an unexpected response
                raise Exception(f"Unexpected response from CryptoCompare API: {data}")
            
            if data['Message'].startswith("cccagg_or_exchange market does not exist for this coin pair"):
                # If the API return an error, check if it's because all the assets are not supported. In this case create an empty list of assets
                found_assets = []
            else:
                # If the API return an other error, raise an exception
                raise Exception(f"Error send by the CryptoCompare API : {data}")
        else:
            found_assets = data.keys()
        
        # print the assets that are not in the response
        missing_assets = set(assetsBatch) - set(found_assets)
        if missing_assets:
            print(f"Unable to get current price for: {missing_assets}")
        
        # Extract prices in data[asset]['USD'] into a pandas Series with asset: price. If the asset is not in the response, enter np.nan as the price.
        prices = pd.Series({asset: data.get(asset, {}).get('USD', np.nan) for asset in assetsBatch})
        
        return prices
    
    @staticmethod
    def addMissingUsdPrice(transactions, apiKey):
        if apiKey is None:
            raise Exception("No API key provided, no USD values will be added")
        # Select transactions with missing usd price
        missingUsdPrice = transactions[transactions['price_USD'].isna()].copy()  # Copy to avoid SettingWithCopyWarning
        # remove the unsuported assets by the api
        missingUsdPrice = missingUsdPrice[~missingUsdPrice['asset'].isin(CryptoCompareWrapper.UnsupportedHistoricalPriceAssets)]
        
        print(f"Remaining missing price to request: {len(missingUsdPrice)}")
        
        if missingUsdPrice.empty:
            return transactions

        # Rename assets to match CryptoCompare API
        missingUsdPrice['asset'] = missingUsdPrice['asset'].replace(CryptoCompareWrapper.AssetNameMap)
        
        # API setup
        api_url = 'https://min-api.cryptocompare.com/data/v2/histohour'
        nWorkers = 3
        rate_limit_delay = 1.0 / 50  # 50 calls per second

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
                time.sleep(rate_limit_delay)  # Add delay to respect rate limit
            
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
        
        return transactions
    
    @staticmethod
    def requestDailyHistoricalPrices(asset: str, apiKey) -> pd.DataFrame:
        if apiKey is None:
            raise Exception("No API key provided, no USD values will be added")
        # Rename asset to match CryptoCompare API
        asset = CryptoCompareWrapper.AssetNameMap.get(asset, asset)
        
        # remove the unsuported assets by the api
        if asset in CryptoCompareWrapper.UnsupportedHistoricalPriceAssets:
            return pd.DataFrame()
        
        # API setup
        api_url = 'https://min-api.cryptocompare.com/data/v2/histoday'
        params={
            'fsym': asset,
            'tsym':'USD',
            'limit': '1',
            'allData':'false',
            'extraParams':'CryptoWallet',
            'apiKey': apiKey,
            'explainPath': 'false'
        }
        
        # get existing historical data from file
        data_filename = f'./data/historical_OHLCV_daily_{asset}.csv'
        saved_data = pd.DataFrame()
        if os.path.exists(data_filename):
            saved_data = pd.read_csv(data_filename, parse_dates=['time'], index_col='time')

            # get the number of days between the last saved data and today
            last_saved_date = saved_data.index.max()
            days_since_last_saved_data = (pd.Timestamp.now(tz='utc') - last_saved_date).days
            if days_since_last_saved_data < 1:
                return saved_data

            params['limit'] = str(days_since_last_saved_data)
            
        else:
            os.makedirs('data', exist_ok=True)
            params['allData'] = 'true'

        
        try:
            response = requests.get(url=api_url,params=params)
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request Error to CryptoCompare API : {e}\n")
        if response.status_code != 200:
            raise Exception(f"Request Error to CryptoCompare API : {response.status_code}")
        
        requested_data = response.json()
        
        # Check if the API response is successful
        if 'Response' not in requested_data.keys():
            raise Exception(f"Unexpected response from CryptoCompare API: {requested_data}")
        if requested_data['Response'] != "Success":
            raise Exception(f"Error send by the CryptoCompare API : {requested_data}")
        
        # Extract the data from the response
        requested_data = pd.DataFrame(requested_data['Data']['Data'])
        # convert "time" to datetime
        requested_data['time'] = pd.to_datetime(requested_data['time'], unit='s', utc=True)
        requested_data.set_index('time', inplace=True)
        # remove row with a price at 0.0 (CryptoCompare API return price from 2010-07-17, even if the asset was not created yet)
        requested_data = requested_data[requested_data['close'] != 0.0]        
        
        # Remove duplicates by ensuring no overlap between saved_data and requested_data
        requested_data = requested_data[~requested_data.index.isin(saved_data.index)]
        # Append the new data to the existing data and save it to a file
        data = pd.concat([saved_data, requested_data])
        data.to_csv(data_filename)
        return data
