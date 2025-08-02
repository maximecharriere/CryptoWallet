import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from .Transaction import Transaction, TransactionType, WalletType
import dataclasses
import os
from copy import deepcopy


class BinanceLoader:
    name = 'Binance'
    TransactionTypesMap = {
        'Deposit': TransactionType.DEPOSIT,
        'Withdraw': TransactionType.WITHDRAW,
        'Buy': TransactionType.SPOT_TRADE,
        'Transaction Buy': TransactionType.SPOT_TRADE,
        'Transaction Revenue': TransactionType.SPOT_TRADE,
        'Sell': TransactionType.SPOT_TRADE,
        'Transaction Sold': TransactionType.SPOT_TRADE,
        'Transaction Spend': TransactionType.SPOT_TRADE,
        'Transaction Related': TransactionType.SPOT_TRADE,
        'Small assets exchange BNB': TransactionType.SPOT_TRADE,
        'Small Assets Exchange BNB': TransactionType.SPOT_TRADE,
        'Fee': TransactionType.FEE,
        'Transaction Fee': TransactionType.FEE,
        'Simple Earn Flexible Interest': TransactionType.SAVING_INTEREST,
        'Simple Earn Flexible Subscription': TransactionType.SAVING_PURCHASE,
        'Simple Earn Flexible Redemption': TransactionType.SAVING_REDEMPTION,
        'Simple Earn Locked Rewards': TransactionType.SAVING_INTEREST,
        'Rewards Distribution': TransactionType.SAVING_INTEREST,
        'Simple Earn Flexible Airdrop': TransactionType.SAVING_INTEREST,
        'Staking Purchase': TransactionType.STAKING_PURCHASE,
        'Staking Rewards': TransactionType.STAKING_INTEREST,
        'Staking Redemption': TransactionType.STAKING_REDEMPTION,
        'ETH 2.0 Staking': TransactionType.STAKING_PURCHASE,
        'ETH 2.0 Staking Rewards': TransactionType.STAKING_INTEREST,
        'ETH 2.0 Staking Withdrawals': TransactionType.STAKING_REDEMPTION,
        'Launchpool Subscription/Redemption': TransactionType.TBD, # To Be Determined
        'Launchpool Airdrop': TransactionType.DISTRIBUTION,
        'Distribution': TransactionType.DISTRIBUTION,
        'Airdrop Assets': TransactionType.DISTRIBUTION,
        'Cash Voucher distribution': TransactionType.DISTRIBUTION,
        'Cash Voucher Distribution': TransactionType.DISTRIBUTION,
        'Commission Fee Shared With You': TransactionType.REFERRAL_INTEREST,
        'Referral Commission': TransactionType.REFERRAL_INTEREST,
        'Referral Kickback': TransactionType.REFERRAL_INTEREST,
        'Token Swap - Redenomination/Rebranding': TransactionType.REDENOMINATION,
        'Token Swap - Distribution': TransactionType.REDENOMINATION,
        'Asset Recovery': TransactionType.REDENOMINATION,
        'Crypto Box': TransactionType.DISTRIBUTION,
        'Binance Convert': TransactionType.SPOT_TRADE,
        'Merchant Acquiring': TransactionType.SPEND,
        'Transfer Between Main and Funding Wallet': TransactionType.ACCOUNT_TRANSFER,
    }

    @classmethod
    def load(cls, filepath_or_buffer) -> pd.DataFrame:
        print(f"Loading transactions from {filepath_or_buffer} file")
        # Check that the file is a csv file
        if (not filepath_or_buffer.endswith('.csv')):
            raise Exception(f"The file {filepath_or_buffer} is not a csv file")
        inTransactions = pd.read_csv(filepath_or_buffer)
        transactions = []
        exceptions_occurred = False
        for idx, row in inTransactions.iterrows():
            try:
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(row['UTC_Time']).replace(tzinfo=timezone.utc),
                    asset=row['Coin'],
                    amount=row['Change'],
                    type=cls.TransactionTypesMap[row['Operation']],
                    exchange=cls.name,
                    userId=str(row['User_ID']),
                    wallet=WalletType(row['Account']),
                    note=f"Operation={row['Operation']}" + ('' if row.isna()['Remark'] else (f", Remark={str(row['Remark'])}"))
                ))

                # Manage all the transaction types that was not managable only with the TransactionTypesMap, and set to TBD.
                if (transactions[-1].type == TransactionType.TBD):
                    if (row['Operation'] == 'Launchpool Subscription/Redemption'):
                        transactions[-1].type = TransactionType.SAVING_PURCHASE if row['Change'] < 0 else TransactionType.SAVING_REDEMPTION
                    else:
                        raise KeyError(row['Operation'])
            except KeyError as e:
                # If a KeyError is raised, it means that the transaction type is not supported by the loader. As many missing transaction type can be missing, we don't raise an exception here. We analyse all the transactions and raise an exception at the end if needed.
                print(f"The transaction type {e} is not supported by the loader")
                exceptions_occurred = True
                continue

            # The BETH coin is the coin representing ETH coins staked in the ETH 2.0 Staking program.
            # Put them in the Staking wallet and remove the prefix.
            # Reward of this program are given in BETH, so are directly staked and stay in the STAKING wallet.
            if (transactions[-1].asset == 'BETH'):
                transactions[-1].wallet = WalletType.STAKING
                transactions[-1].note += ', Original asset is BETH'
                transactions[-1].asset = 'ETH'

            # In binance the SAVING wallet does not belong to the user.
            # So during a saving purchase or redemption, there is a transaction telling the in/out flow of the SPOT wallet,
            # but it does not say the in/out flow of the SAVING wallet.
            # This transaction is therefore added by this program.
            if (transactions[-1].type in {TransactionType.SAVING_PURCHASE, TransactionType.SAVING_REDEMPTION}):
                transactions.append(dataclasses.replace(transactions[-1],
                                                        wallet=WalletType.SAVING,
                                                        amount=-transactions[-1].amount,
                                                        note=transactions[-1].note + ', Transaction not from Binance'))

            # In binance the STAKING wallet does not belong to the user.
            # So during a staking purchase or redemption, there is a transaction telling the in/out flow of the SPOT wallet,
            # but it does not say the in/out flow of the STAKING wallet.
            # This transaction is therefore added by this program.
            # However Binance store the ETH 2.0 Staking in the SPOT wallet with the BETH coin. Don't create a new transaction
            # for ETH 2.0 Staking transactions
            if (transactions[-1].type in {TransactionType.STAKING_PURCHASE, TransactionType.STAKING_REDEMPTION}
                    and row['Operation'] not in {'ETH 2.0 Staking', 'ETH 2.0 Staking Withdrawals'}):
                transactions.append(dataclasses.replace(transactions[-1],
                                                        wallet=WalletType.STAKING,
                                                        amount=-
                                                        transactions[-1].amount,
                                                        note=transactions[-1].note + ', Transaction not from Binance'))

        transactions_df = pd.DataFrame(transactions)
        
        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions_df


class LedgerLoader:
    name = 'Ledger'
    TransactionTypesMap = {
        'IN': TransactionType.DEPOSIT,
        'OUT': TransactionType.WITHDRAW,
        'NFT_IN': TransactionType.TBD,
        'FEES': TransactionType.FEE
    }
    
    @classmethod
    def load(cls, filepath_or_buffer) -> pd.DataFrame:
        print(f"Loading transactions from {filepath_or_buffer} file")
        # Check that the file is a csv file
        if (not filepath_or_buffer.endswith('.csv')):
            raise Exception(f"The file {filepath_or_buffer} is not a csv file")
        inTransactions = pd.read_csv(filepath_or_buffer)
        transactions = []
        exceptions_occurred = False
        for idx, row in inTransactions.iterrows():
            if (row['Status'] != 'Confirmed'):
                if (row['Status'] != 'Failed'):
                    raise Exception(
                        f"The transaction status '{row['Status']}' is unknown. Only 'Confirmed' and 'Failed' are supported by the loader.")
                continue  # Skip the transaction if it is not confirmed
            try:
                new_transaction = Transaction(
                    datetime=datetime.fromisoformat(row['Operation Date']).replace(tzinfo=timezone.utc),
                    asset=row['Currency Ticker'],
                    amount=row['Operation Amount'],
                    type=cls.TransactionTypesMap[row['Operation Type']],
                    exchange=cls.name,
                    userId=str(row['Account Name']) + ' - ' + str(row['Account xpub']),
                    wallet=WalletType.FUNDING,
                    note=f"Operation Hash={row['Operation Hash']}"
                )
                # If the transaction type is OUT, the amount is negative
                if (new_transaction.type in {TransactionType.WITHDRAW, TransactionType.FEE}):
                    new_transaction.amount = -new_transaction.amount
                # Manage all the transaction types that was not managable only with the TransactionTypesMap, and set to TBD.
                if (new_transaction.type == TransactionType.TBD):
                    if (row['Operation Type'] == 'NFT_IN'):
                        # NFT_IN is a transaction type that is not supported by the loader. Go to the next transaction.
                        continue
                    else:
                        raise KeyError(row['Operation'])
                
                transactions.append(new_transaction)
                
                # If the transaction fee is not 0, add a new transaction for the fee
                if ((row['Operation Fees'] != 0) & (new_transaction.type == TransactionType.WITHDRAW)):
                    transactions.append(dataclasses.replace(new_transaction,
                                                            amount=-row['Operation Fees'],
                                                            type=TransactionType.FEE,
                                                            note=new_transaction.note +', Fee'))
                    
            except KeyError as e:
                # If a KeyError is raised, it means that the transaction type is not supported by the loader. As many missing transaction type can be missing, we don't raise an exception here. We analyse all the transactions and raise an exception at the end if needed.
                print(f"The transaction type {e} is not supported by the loader")
                exceptions_occurred = True
                continue

        transactions_df = pd.DataFrame(transactions)
        
        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions_df
    

class CoinbaseLoader:
    name = 'Coinbase'
    TransactionTypesMap = {
        'Send': TransactionType.WITHDRAW,
        'Deposit': TransactionType.DEPOSIT,
        'Receive': TransactionType.REFERRAL_INTEREST,
        'Convert': TransactionType.SPOT_TRADE,
        'Buy': TransactionType.SPOT_TRADE
    }
    NegativeTransactionTypes = {'Send', 'Convert'}

    @classmethod
    def load(cls, filepath_or_buffer) -> pd.DataFrame:
        raise DeprecationWarning("CoinbaseLoader is not supported anymore. Use the ManualTransactionsLoader instead.")
        print(f"Loading transactions from {filepath_or_buffer} file")
        # Check that the file is a csv file
        if (not filepath_or_buffer.endswith('.csv')):
            raise Exception(f"The file {filepath_or_buffer} is not a csv file")
        transactions = []
        exceptions_occurred = False
        inTransactions = pd.read_csv(
            filepath_or_buffer, 
            skiprows=3,
            converters={
                'Timestamp': lambda value: datetime.strptime(value, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc),
                'Price at Transaction': cls.strip_currency,
                'Subtotal': cls.strip_currency,
                'Total (inclusive of fees and/or spread)': cls.strip_currency,
                'Fees and/or Spread': cls.strip_currency
            })
        if inTransactions.empty:
            return pd.DataFrame(transactions)
        
        # Get UID from the first row
        with open(filepath_or_buffer, 'r') as f:
            for line in f:
                if line.startswith("User,"):
                    uid = line.split(',')[2].strip()
                    break  # Exit the loop once the UID is found
        if uid is None:
            raise Exception("UID not found in the file")
            
            
        for idx, row in inTransactions.iterrows():
            try:
                transaction = Transaction(
                    datetime=row['Timestamp'],
                    asset=row['Asset'],
                    amount= -row['Quantity Transacted'] if row['Transaction Type'] in cls.NegativeTransactionTypes else row['Quantity Transacted'],
                    type=cls.TransactionTypesMap[row['Transaction Type']],
                    exchange=cls.name,
                    userId=uid,
                    wallet=WalletType.SPOT,  # Default transaction are done with the Spot wallet
                    note=f"Transaction Type={row['Transaction Type']}" + ('' if row.isna()['Notes'] else (f", Notes={str(row['Notes'])}")),
                    price_USD=row['Price at Transaction'],
                    amount_USD=-row['Subtotal'] if row['Transaction Type'] in cls.NegativeTransactionTypes else row['Subtotal']
                )

                # Manage all the transaction types that was not managable only with the TransactionTypesMap, and set to TBD.
                if (transaction.type == TransactionType.TBD):
                    raise KeyError(row['Operation'])
                
                transactions.append(deepcopy(transaction))
                
            except KeyError as e:
                # If a KeyError is raised, it means that the transaction type is not supported by the loader. As many missing transaction type can be missing, we don't raise an exception here. We analyse all the transactions and raise an exception at the end if needed.
                print(f"The transaction type {e} is not supported by the loader")
                exceptions_occurred = True
                continue

            if (row['Transaction Type'] == 'Buy'):
                # Buy transaction directly buy crypto from USD fiat money. The transaction is written in only one line, but is done in four steps:
                # 1. Deposit of USD fiat on the account
                # 2. Sell of USD fiat
                # 3. Buy of crypto
                # 4. Fees for the buy transaction
                transactions.append(dataclasses.replace(transaction,
                                                            asset='USD',
                                                            amount=row['Total (inclusive of fees and/or spread)'],
                                                            type=TransactionType.DEPOSIT,
                                                            note=transaction.note + ', Step 1: Initial USD deposit for Buy transaction',
                                                            price_USD=1,
                                                            amount_USD=row['Total (inclusive of fees and/or spread)'])
                )
                transactions.append(dataclasses.replace(transaction,
                                                            asset='USD',
                                                            amount=-row['Subtotal'],
                                                            type=TransactionType.SPOT_TRADE,
                                                            note=transaction.note + ', Step 2: Sell USD for Buy transaction',
                                                            price_USD=1,
                                                            amount_USD=-row['Subtotal'])
                )
                transactions[-3].note = transactions[-3].note + ', Step 3: Buy crypto for Buy transaction'

                transactions.append(dataclasses.replace(transaction,
                                                            asset='USD',
                                                            amount=-row['Fees and/or Spread'],
                                                            type=TransactionType.FEE,
                                                            note=transaction.note + ', Step 4: Fees for Buy transaction',
                                                            price_USD=1,
                                                            amount_USD=-row['Fees and/or Spread'])
                )
            elif (row['Transaction Type'] == 'Convert'):
                # Convert transaction convert crypto from one to another. The transaction is written in only one line, but is done in three steps:
                # 1. Sell of crypto
                # 2. Buy of crypto
                # 3. Fees for the convert transaction
                fee_by_asset_currency = row['Fees and/or Spread'] / row['Price at Transaction'] # calculate the fees in the asset currency and not USD
                bought_asset_name = row['Notes'].split()[-1]
                bought_asset_amount = float(row['Notes'].split()[-2])
                
                transactions[-1].amount = transactions[-1].amount + fee_by_asset_currency # remove the fees from the sold amount of the sell transaction
                transactions[-1].note = transactions[-1].note + ', Step 1: Sell crypto for Convert transaction'
                
                transactions.append(dataclasses.replace(transaction,
                                                            asset=bought_asset_name,
                                                            amount=bought_asset_amount,
                                                            type=TransactionType.SPOT_TRADE,
                                                            note=transaction.note + ', Step 2: Buy crypto for Convert transaction',
                                                            price_USD=row['Subtotal']/bought_asset_amount)
                )
                
                transactions.append(dataclasses.replace(transaction,
                                                            amount=-fee_by_asset_currency,
                                                            type=TransactionType.FEE,
                                                            note=transaction.note + ', Step 3: Fees for Convert transaction',
                                                            price_USD=row['Price at Transaction'],
                                                            amount_USD=-row['Fees and/or Spread'])
                )
            
            elif (row['Fees and/or Spread'] != 0):
                raise Exception("Fees different from 0 for a transaction that is not a 'Buy' or 'Convert' transaction is not supported by the loader.")


        transactions_df = pd.DataFrame(transactions)
        
        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions_df
    
    @staticmethod
    def strip_currency(value):
        """
        Strips the dollar sign and converts the value to a float.
        Handles missing or invalid data gracefully.
        """
        try:
            return float(value.replace('$', '').strip())
        except ValueError:
            return None  # Return None for invalid or empty values    
    
    
class ManualTransactionsLoader:
    @classmethod
    def load(cls, filepath_or_buffer) -> pd.DataFrame:
        print(f"Loading transactions from {filepath_or_buffer} file")
        #Check that filepath_or_buffer exists
        if not os.path.exists(filepath_or_buffer):
            raise FileNotFoundError(f"File {filepath_or_buffer} not found")
        # Check that the file is a csv file
        if (not filepath_or_buffer.endswith('.csv')):
            raise Exception(f"The file {filepath_or_buffer} is not a csv file")
        inTransactions = pd.read_csv(filepath_or_buffer, parse_dates=['datetime'], date_format='ISO8601', converters={
            'type' : lambda s: TransactionType[s],
            'wallet' : lambda s: WalletType[s]
        })

        return inTransactions


class SwissborgLoader:
    name = 'Swissborg'
    TransactionTypesMap = {
        'Deposit': TransactionType.DEPOSIT,
        'Withdrawal': TransactionType.WITHDRAW,
        'Buy': TransactionType.SPOT_TRADE,
        'Sell': TransactionType.SPOT_TRADE,
        'Payouts': TransactionType.STAKING_INTEREST
    }
    NegativeTransactionTypes = {'Withdrawal', 'Sell'}
    
    @classmethod
    def load(cls, filepath_or_buffer) -> pd.DataFrame:
        print(f"Loading transactions from {filepath_or_buffer} file")
        # Check that the file is a xlsx file
        if (not filepath_or_buffer.endswith('.xlsx')):
            raise Exception(
                f"The file {filepath_or_buffer} is not a xlsx file")

        inTransactions = pd.read_excel(
            filepath_or_buffer, header=13, usecols='A:K')
        # The user id is in the cell 6E row of the file
        userId = pd.read_excel(
            filepath_or_buffer, usecols="E", skiprows=4, nrows=1).iat[0, 0]

        transactions = []
        exceptions_occurred = False
        for idx, row in inTransactions.iterrows():
            try:
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(
                        row['Time in UTC']).replace(tzinfo=timezone.utc),
                    asset=row['Currency'],
                    amount=-row['Gross amount'] if row['Type'] in cls.NegativeTransactionTypes else row['Gross amount'],
                    type=cls.TransactionTypesMap[row['Type']],
                    exchange=cls.name,
                    userId=userId,
                    wallet=WalletType.SPOT,  # Default transaction are done with the Spot wallet
                    note=f"Type={row['Type']}" + ('' if row.isna()['Note'] else (f", Note={str(row['Note'])}")),
                    price_USD=row['Gross amount (USD)']/row['Gross amount'],
                    amount_USD=- row['Gross amount (USD)'] if row['Type'] in cls.NegativeTransactionTypes else row['Gross amount (USD)']
                ))
                # If the transaction fee is not 0, add a new transaction for the fee
                if (row['Fee'] != 0):
                    transactions.append(dataclasses.replace(transactions[-1],
                                                            amount=-row['Fee'],
                                                            type=TransactionType.FEE,
                                                            note=transactions[-1].note +
                                                            ', Fee',
                                                            amount_USD=-row['Fee (USD)']))

            except KeyError as e:
                print(f"The transaction type {e} is not supported by the loader")
                exceptions_occurred = True

        transactions_df = pd.DataFrame(transactions)
        
        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions_df
    
class KucoinLoader:
    name = 'Kucoin'
    TransactionTypesMap = {
            'Deposit': TransactionType.DEPOSIT,
            'Withdraw': TransactionType.WITHDRAW,
            'Withdrawal': TransactionType.WITHDRAW,
            'Transfer': TransactionType.ACCOUNT_TRANSFER,
            'Rewards': TransactionType.DISTRIBUTION,
            'KCS Bonus': TransactionType.DISTRIBUTION,
            'Fiat Deposit': TransactionType.DEPOSIT,
            'KuCoin Event': TransactionType.TBD,
            'Spot': TransactionType.SPOT_TRADE,
            'Fee Refunds using KCS': TransactionType.SPOT_TRADE,
            'KCS Fee Deduction': TransactionType.SPOT_TRADE,
        }
    
    @classmethod
    def load(cls, folderpath) -> pd.DataFrame:
        print(f"Loading transactions from {folderpath} folder")
        # Check that the folderpath is a folder
        if not os.path.isdir(folderpath):
            raise Exception(f"The path {folderpath} is not a folder. Kucoin store multiple CSV files in a folder")

        csv_files = [file for file in os.listdir(folderpath) if file.endswith('.csv')]
        transactions = []
        exceptions_occurred = False
        for file in csv_files:
            print(f"- Reading '{file}'")
            filepath = os.path.join(folderpath, file)
            inTransactions = pd.read_csv(filepath)
            if inTransactions.empty:
                continue

            # Get the wallet type from the file name
            if file.startswith("Account History_Funding Account"):
                walletType = WalletType.FUNDING
            elif file.startswith("Account History_Trading Account"):
                walletType = WalletType.SPOT
            elif file.startswith("Account History_Cross Margin Account"):
                raise Exception("The Cross Margin Account is not supported by the loader.")
            elif file.startswith("Account History_Isolated Margin Account"):
                raise Exception("The Isolated Margin Account is not supported by the loader.")
            else :
                print(f"The file '{file}' is skipped. Only 'Account History' files are used by the loader.")
                continue

            # Get the time column name and timezone offset
            time_column_name, timezone_offset_minutes  = cls.get_time_offset(inTransactions)

            for idx, row in inTransactions.iterrows():
                try:
                    # manage amount sign
                    if (row['Side'] == 'Deposit'):
                        amount=row['Amount']
                    elif (row['Side'] == 'Withdrawal'):
                        amount=-row['Amount']
                    else:
                        raise KeyError(row['Side'])
                    
                    gross_amount = amount + row['Fee'] # Kucoin already substract the fee from the amount, so we need to add it back to get the gross amount
                    
                    transactions.append(Transaction(
                        datetime=datetime.fromisoformat(str(row[time_column_name])).replace(tzinfo=timezone(
                            timedelta(minutes=timezone_offset_minutes))).astimezone(timezone.utc),
                        asset=row['Currency'],
                        amount=gross_amount,
                        type=cls.TransactionTypesMap[row['Type']],
                        exchange=cls.name,
                        userId=row['UID'],
                        wallet=walletType,
                        note=f"Remark={row['Remark']}, Type={row['Type']}, Side={row['Side']}"
                    ))
                    
                    if (transactions[-1].type == TransactionType.TBD):
                        # if the type is a transfer between two internal account, set the type to DEPOSIT or WITHDRAW
                        if (row['Type'] == 'KuCoin Event'):
                            transactions[-1].type = cls.TransactionTypesMap[row['Side']]
                        else:
                            raise KeyError(row['Type'])
                    
                except KeyError as e:
                    # If a KeyError is raised, it means that the transaction type is not supported by the loader. As many missing transaction type can be missing, we don't raise an exception here. We analyse all the transactions and raise an exception at the end if needed.
                    print(f"The key '{e}' is not supported by the loader")
                    exceptions_occurred = True
                    continue
                
                # If the transaction fee is not 0, add a new transaction for the fee
                if (row['Fee'] != 0):
                    transactions.append(dataclasses.replace(transactions[-1],
                                                            amount=-row['Fee'],
                                                            type=TransactionType.FEE,
                                                            note=transactions[-1].note + ', Fee'))

        transactions_df = pd.DataFrame(transactions)
        
        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")
        return transactions_df
    
    
    @classmethod
    def get_time_offset(cls, data):
        # Get the name of the column which has the format 'Time(UTC+02:00)'
        time_column_name = [col for col in data.columns if 'Time(UTC' in col][0]
        # Extract the sign, hours, and minutes from the timezone string
        sign = time_column_name[8]
        hours = int(time_column_name[9:11])
        minutes = int(time_column_name[12:14])
        # Calculate the total offset in minutes
        timezone_offset_minutes = (hours * 60 + minutes) * (-1 if sign == '-' else 1)
        return time_column_name, timezone_offset_minutes


class BybitLoader:
    name = 'Bybit'
    TransactionTypesMap = {
            'Deposit': TransactionType.DEPOSIT,
            'Withdrawal': TransactionType.WITHDRAW,
            'Transfer from Unified Trading Account': TransactionType.ACCOUNT_TRANSFER,
            'Transfer to Unified Trading Account': TransactionType.ACCOUNT_TRANSFER,
            'Launchpool Manual Withdrawal': TransactionType.SAVING_REDEMPTION,
            'Launchpool Yield': TransactionType.SAVING_INTEREST,
            'Launchpool Subscription': TransactionType.SAVING_PURCHASE,
            'Earn': TransactionType.DISTRIBUTION,
            'TRADE' : TransactionType.SPOT_TRADE,
            'TRANSFER_OUT' : TransactionType.ACCOUNT_TRANSFER,
            'TRANSFER_IN' : TransactionType.ACCOUNT_TRANSFER,
        }
    StableCoinsUSD = {'USDT', 'USDC', 'DAI', 'BUSD', 'USD', 'USDS', 'USDe', 'FDUSD', 'USDD', 'PYUSD', 'TUSD'}
    
    @classmethod
    def load(cls, folderpath) -> pd.DataFrame:
        print(f"Loading transactions from {folderpath} folder")
        # Check that the folderpath is a folder
        if not os.path.isdir(folderpath):
            raise Exception(f"The path {folderpath} is not a folder. Bybit store multiple CSV files in a folder")

        csv_files = [file for file in os.listdir(folderpath) if file.endswith('.csv')]
        if not csv_files:
            raise Exception(f"No CSV files found in the folder {folderpath}")
        transactions = []
        exceptions_occurred = False
        for file in csv_files:
            print(f"- Reading '{file}'")
            filepath = os.path.join(folderpath, file)
                
            # Get the wallet type from the file name
            if file.startswith("Bybit_AssetChangeDetails_fund"):
                transactions_funding, exceptions_funding = cls.load_funding(filepath)
                transactions += transactions_funding
                exceptions_occurred = exceptions_occurred or exceptions_funding
            elif file.startswith("Bybit_AssetChangeDetails_uta"):
                transactions_spot, exceptions_spot = cls.load_spot(filepath)
                transactions += transactions_spot
                exceptions_occurred = exceptions_occurred or exceptions_spot
            else :
                raise Exception(f"The file '{file}' is not supported by the loader.")
            
            
        transactions_df = pd.DataFrame(transactions)
        
        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")
            
        return transactions_df
    
    @classmethod
    def load_funding(cls, filepath):
        transactions = []
        exceptions_occurred = False
        inTransactions = pd.read_csv(filepath, skiprows=1)
        if inTransactions.empty:
            return transactions, exceptions_occurred
        
        # Get UID from the first row
        with open(filepath) as f:
            uid = f.readline().split(',')[0].split(':')[1].strip()
            
        for idx, row in inTransactions.iterrows():
            try:                
                transaction = Transaction(
                    datetime=datetime.fromisoformat(str(row['Date & Time(UTC)'])).astimezone(timezone.utc),
                    asset=row['Coin'],
                    amount=row['QTY'],
                    type=cls.TransactionTypesMap[row['Description']],
                    exchange=cls.name,
                    userId=uid,
                    wallet=WalletType.FUNDING,
                    note=f"Description={row['Description']}, Type={row['Type']}"
                )
                
                if (transaction.type == TransactionType.TBD):
                    raise KeyError(row['Description'])
                
                transactions.append(transaction)
                
            except KeyError as e:
                # If a KeyError is raised, it means that the transaction type is not supported by the loader. As many missing transaction type can be missing, we don't raise an exception here. We analyse all the transactions and raise an exception at the end if needed.
                print(f"The key '{e}' is not supported by the loader")
                exceptions_occurred = True
                continue
            
            if (transaction.type in {TransactionType.SAVING_PURCHASE, TransactionType.SAVING_REDEMPTION}):
                transactions.append(dataclasses.replace(transaction,
                                                        wallet=WalletType.SAVING,
                                                        amount=-transaction.amount,
                                                        note=transaction.note + ', Transaction not from Bybit'))
                
        return transactions, exceptions_occurred

    @classmethod
    def load_spot(cls, filepath):
        transactions = []
        exceptions_occurred = False
        inTransactions = pd.read_csv(filepath, skiprows=1)
        if inTransactions.empty:
            return transactions, exceptions_occurred
        
        # Get UID from the first row
        with open(filepath) as f:
            uid = f.readline().split(',')[0].split(':')[1].strip()
            
        for idx, row in inTransactions.iterrows():
            try:
                # Try to get the price of the asset
                # Set price_USD to the 'Filled Price' if the 'Currency' is not an USD stablecoin, but a stablecoin is present in the trading pair ('Contract' column)
                # Otherwise, set price_USD to NaN.
                price_USD = row['Filled Price'] if (row['Currency'] not in cls.StableCoinsUSD and pd.notna(row['Contract']) and any(coin in row['Contract'] for coin in cls.StableCoinsUSD)) else np.nan
                    
                transaction = Transaction(
                    datetime=datetime.fromisoformat(str(row['Time(UTC)'])).astimezone(timezone.utc),
                    asset=row['Currency'],
                    amount=row['Cash Flow'],
                    type= cls.TransactionTypesMap[row['Type']],
                    exchange=cls.name,
                    userId=uid,
                    wallet=WalletType.SPOT,
                    note=f"Contract={row['Contract']}, Direction={row['Direction']}",
                    price_USD=price_USD,
                    amount_USD=row['Cash Flow']*price_USD
                )
                transactions.append(transaction)
                
                # If the transaction fee is not 0, add a new transaction for the fee
                if (row['Fee Paid'] != 0):
                    transactions.append(dataclasses.replace(transaction,
                                                        amount=row['Fee Paid'],
                                                        type=TransactionType.FEE,
                                                        note=transaction.note +', Fee',
                                                        amount_USD=row['Fee Paid']*price_USD))

            except KeyError as e:
                # If a KeyError is raised, it means that the transaction type is not supported by the loader. As many missing transaction type can be missing, we don't raise an exception here. We analyse all the transactions and raise an exception at the end if needed.
                print(f"The key '{e}' is not supported by the loader")
                exceptions_occurred = True
                continue
                        


        return transactions, exceptions_occurred
    
