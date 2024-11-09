import pandas as pd
from datetime import datetime, timezone, timedelta
from .Transaction import Transaction, TransactionType, WalletType
import dataclasses
import os


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
        'Merchant Acquiring': TransactionType.SPEND
    }
    CryptoNameMap = {
        'SHIB2': 'SHIB'
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
                    datetime=datetime.fromisoformat(
                        row['UTC_Time']).replace(tzinfo=timezone.utc),
                    asset=row['Coin'],
                    amount=row['Change'],
                    type=cls.TransactionTypesMap[row['Operation']],
                    exchange=cls.name,
                    userId=str(row['User_ID']),
                    wallet=WalletType.SPOT,  # Default transaction are done with the Spot wallet
                    note=f"Operation={
                        row['Operation']}" + ('' if row.isna()['Remark'] else (f", Remark={str(row['Remark'])}"))
                ))

                # Manage all the transaction types that was not managable only with the TransactionTypesMap, and set to TBD.
                if (transactions[-1].type == TransactionType.TBD):
                    if (row['Operation'] == 'Launchpool Subscription/Redemption'):
                        transactions[-1].type = TransactionType.SAVING_PURCHASE if row['Change'] < 0 else TransactionType.SAVING_REDEMPTION
                    else:
                        raise KeyError(row['Operation'])
            except KeyError as e:
                print(f"The transaction type {e} is not supported by the loader")
                exceptions_occurred = True

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
                                                        amount=-
                                                        transactions[-1].amount,
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
        transactions_df['asset'] = transactions_df['asset'].map(
            lambda s: cls.CryptoNameMap[s] if s in cls.CryptoNameMap else s)

        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions_df


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
                    amount=-
                    row['Gross amount'] if row['Type'] in cls.NegativeTransactionTypes else row['Gross amount'],
                    type=cls.TransactionTypesMap[row['Type']],
                    exchange=cls.name,
                    userId=userId,
                    wallet=WalletType.SPOT,  # Default transaction are done with the Spot wallet
                    note=f"Type={
                        row['Type']}" + ('' if row.isna()['Note'] else (f", Note={str(row['Note'])}")),
                    price_USD=row['Gross amount (USD)']/row['Gross amount'],
                    amount_USD=- \
                    row['Gross amount (USD)'] if row['Type'] in cls.NegativeTransactionTypes else row['Gross amount (USD)'],
                ))
                # If the transaction fee is not 0, add a new transaction for the fee
                if (row['Fee'] != 0):
                    transactions.append(dataclasses.replace(transactions[-1],
                                                            amount=row['Fee'],
                                                            type=TransactionType.FEE,
                                                            note=transactions[-1].note +
                                                            ', Fee',
                                                            amount_USD=row['Fee (USD)']))

            except KeyError as e:
                print(f"The transaction type {
                      e} is not supported by the loader")
                exceptions_occurred = True

        transactions_df = pd.DataFrame(transactions)

        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions_df


class KucoinLoader:
    name = 'Kucoin'

    @classmethod
    def load(cls, folderpath) -> pd.DataFrame:
        print(f"Loading transactions from {folderpath} folder")
        # Check that the folderpath is a folder
        if not os.path.isdir(folderpath):
            raise Exception(f"The path {folderpath} is not a folder. Kucoin store multiple CSV files in a folder")

        csv_files = [file for file in os.listdir(
            folderpath) if file.endswith('.csv')]
        transactions = []
        for file in csv_files:
            print(f"- Reading '{file}'")
            filepath = os.path.join(folderpath, file)
            df = pd.read_csv(filepath)
            if df.empty:
                continue

            if file.startswith("Deposit_Withdrawal History_Deposit History"):
                transactions.extend(cls.load_deposit(df))
            elif file.startswith("Deposit_Withdrawal History_Withdrawal Record"):
                transactions.extend(cls.load_withdrawal(df))
            elif file.startswith("Spot Orders_Filled Orders (Show Order-Splitting)"):
                pass
            elif file.startswith("Spot Orders_Filled Orders"):
                transactions.extend(cls.load_spot_orders(df))
            elif file.startswith("Account History_Funding Account"):
                transactions.extend(cls.load_funding_account(df))
            elif file.startswith("Others_Asset Snapshots"):
                pass
            elif file.startswith("Fiat Orders_Fiat Deposits"):
                transactions.extend(cls.load_fiat_deposit(df, filepath))
            elif file.startswith("Fiat Orders_Fiat Withdrawals"):
                transactions.extend(cls.load_fiat_withdrawal(df, filepath))
            elif file.startswith("Trading Bot_Filled Orders (Show Order-Splitting)_Spot"):
                pass
            else:
                raise Exception(
                    f"The file '{file}' is not supported by the loader and is not empty.")

        transactions_df = pd.DataFrame(transactions)

        return transactions_df

    @classmethod
    def load_deposit(cls, inTransactions) -> list[Transaction]:
        transactions = []
        exceptions_occurred = False

        # Get the name of the column which has the format 'Time(UTC+02:00)'
        time_column_name = [
            col for col in inTransactions.columns if 'Time(UTC' in col][0]

        # Extract the sign, hours, and minutes from the timezone string
        sign = time_column_name[8]
        hours = int(time_column_name[9:11])
        minutes = int(time_column_name[12:14])
        # Calculate the total offset in minutes
        timezone_offset_minutes = (
            hours * 60 + minutes) * (-1 if sign == '-' else 1)

        for idx, row in inTransactions.iterrows():
            try:
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(row[time_column_name]).replace(tzinfo=timezone(
                        timedelta(minutes=timezone_offset_minutes))).astimezone(timezone.utc),
                    asset=row['Coin'],
                    amount=row['Amount'],
                    type=TransactionType.DEPOSIT,
                    exchange=cls.name,
                    userId=row['UID'],
                    wallet=WalletType.SPOT,  # Default transaction are done with the Spot wallet
                    note=f"Remarks={row['Remarks']}"
                ))
                # If the transaction fee is not 0, add a new transaction for the fee
                if (row['Fee'] != 0):
                    transactions.append(dataclasses.replace(transactions[-1],
                                                            amount=row['Fee'],
                                                            type=TransactionType.FEE,
                                                            note=transactions[-1].note + ', Fee'))

            except KeyError as e:
                print(f"The transaction type {
                      e} is not supported by the loader")
                exceptions_occurred = True

        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions

    @classmethod
    def load_withdrawal(cls, inTransactions) -> list[Transaction]:
        transactions = []
        exceptions_occurred = False

        # Get the name of the column which has the format 'Time(UTC+02:00)'
        time_column_name = [
            col for col in inTransactions.columns if 'Time(UTC' in col][0]

        # Extract the sign, hours, and minutes from the timezone string
        sign = time_column_name[8]
        hours = int(time_column_name[9:11])
        minutes = int(time_column_name[12:14])
        # Calculate the total offset in minutes
        timezone_offset_minutes = (
            hours * 60 + minutes) * (-1 if sign == '-' else 1)

        for idx, row in inTransactions.iterrows():
            try:
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(row[time_column_name]).replace(tzinfo=timezone(
                        timedelta(minutes=timezone_offset_minutes))).astimezone(timezone.utc),
                    asset=row['Coin'],
                    amount=-row['Amount'],
                    type=TransactionType.WITHDRAW,
                    exchange=cls.name,
                    userId=row['UID'],
                    wallet=WalletType.SPOT,  # Default transaction are done with the Spot wallet
                    note=f"Remarks={row['Remarks']}"
                ))
                # If the transaction fee is not 0, add a new transaction for the fee
                if (row['Fee'] != 0):
                    transactions.append(dataclasses.replace(transactions[-1],
                                                            amount=row['Fee'],
                                                            type=TransactionType.FEE,
                                                            note=transactions[-1].note + ', Fee'))

            except KeyError as e:
                print(f"The transaction type {
                      e} is not supported by the loader")
                exceptions_occurred = True

        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions

    @classmethod
    def load_spot_orders(cls, inTransactions) -> list[Transaction]:
        transactions = []
        exceptions_occurred = False

        # Get the name of the column which has the format 'Filled Time(UTC+02:00)'
        time_column_name = [
            col for col in inTransactions.columns if 'Filled Time(UTC' in col][0]

        # Extract the sign, hours, and minutes from the timezone string
        sign = time_column_name[15]
        hours = int(time_column_name[16:18])
        minutes = int(time_column_name[19:21])
        # Calculate the total offset in minutes
        timezone_offset_minutes = (
            hours * 60 + minutes) * (-1 if sign == '-' else 1)

        for idx, row in inTransactions.iterrows():
            try:
                # The trading pair is in the format 'BTC-USDT' in the column 'Symbol'. Slit it to get the two assets
                asset1, asset2 = row['Symbol'].split('-')
                # Only SELL and BUY transactions are supported
                if row['Side'] not in {'SELL', 'BUY'}:
                    raise KeyError(row['Side'])
                # Add transaction for the first asset
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(row[time_column_name]).replace(tzinfo=timezone(
                        timedelta(minutes=timezone_offset_minutes))).astimezone(timezone.utc),
                    asset=asset1,
                    amount=row['Filled Amount'] if row['Side'] == 'BUY' else -
                    row['Filled Amount'],
                    type=TransactionType.SPOT_TRADE,
                    exchange=cls.name,
                    userId=row['UID'],
                    wallet=WalletType.SPOT,  # Default transaction are done with the Spot wallet
                    note=f"Symbol={row['Symbol']}, Side={row['Side']}",
                    price_USD=row['Filled Volume (USDT)']/row['Filled Amount'],
                    amount_USD=row['Filled Volume (USDT)'] if row['Side'] == 'BUY' else - \
                    row['Filled Volume (USDT)']
                ))
                # Add transaction for the second asset
                transactions.append(dataclasses.replace(transactions[-1],
                                                        asset=asset2,
                                                        amount=row['Filled Volume'] if row['Side'] == 'SELL' else -
                                                        row['Filled Volume'],
                                                        price_USD=row['Filled Volume (USDT)'] /
                                                        row['Filled Volume'],
                                                        amount_USD=row['Filled Volume (USDT)'] if row['Side'] == 'SELL' else -
                                                        row['Filled Volume (USDT)']
                                                        ))
                # If the transaction fee is not 0, add a new transaction for the fee
                if (row['Fee'] != 0):
                    transactions.append(dataclasses.replace(transactions[-1],
                                                            asset=row['Fee Currency'],
                                                            amount=row['Fee'],
                                                            type=TransactionType.FEE,
                                                            note=transactions[-1].note +
                                                            ', Fee',
                                                            price_USD=None,
                                                            amount_USD=None
                                                            ))

            except KeyError as e:
                print(f"The transaction type {
                      e} is not supported by the loader")
                exceptions_occurred = True

        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions

    @classmethod
    def load_funding_account(cls, inTransactions) -> list[Transaction]:
        transactions = []
        exceptions_occurred = False

        # Get the name of the column which has the format 'Time(UTC+02:00)'
        time_column_name = [
            col for col in inTransactions.columns if 'Time(UTC' in col][0]

        # Extract the sign, hours, and minutes from the timezone string
        sign = time_column_name[8]
        hours = int(time_column_name[9:11])
        minutes = int(time_column_name[12:14])
        # Calculate the total offset in minutes
        timezone_offset_minutes = (
            hours * 60 + minutes) * (-1 if sign == '-' else 1)

        for idx, row in inTransactions.iterrows():
            try:
                # Only DISTRIBUTION transactions are supported
                if not 'BONUS' in row['Remark'].upper():
                    raise KeyError('Funding Account')
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(row[time_column_name]).replace(tzinfo=timezone(
                        timedelta(minutes=timezone_offset_minutes))).astimezone(timezone.utc),
                    asset=row['Currency'],
                    amount=row['Amount'],
                    # the type of the transaction is TransactionType.DISTRIBUTION if the Remark include the word BONUS, bonus or Bonus
                    type=TransactionType.DISTRIBUTION,
                    exchange=cls.name,
                    userId=row['UID'],
                    wallet=WalletType.SPOT,  # Default transaction are done with the Spot wallet
                    note=f"Side={
                        row['Side']}" + ('' if row.isna()['Remark'] else (f", Remark={str(row['Remark'])}"))
                ))
                # If the transaction fee is not 0, add a new transaction for the fee
                if (row['Fee'] != 0):
                    transactions.append(dataclasses.replace(transactions[-1],
                                                            amount=row['Fee'],
                                                            type=TransactionType.FEE,
                                                            note=transactions[-1].note + ', Fee'))

            except KeyError as e:
                print(f"The transaction type {
                      e} is not supported by the loader")
                exceptions_occurred = True

        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions

    @classmethod
    def load_fiat_deposit(cls, inTransactions, filepath) -> list[Transaction]:
        transactions = []
        exceptions_occurred = False

        # Get time column name and timezone offset
        # Get the name of the column which has the format 'Time(UTC+hh:mm)'
        time_column_name = [
            col for col in inTransactions.columns if 'Time(UTC' in col][0]

        # Extract the sign, hours, and minutes from the timezone string
        sign = time_column_name[8]
        hours = int(time_column_name[9:11])
        minutes = int(time_column_name[12:14])
        # Calculate the total offset in minutes
        timezone_offset_minutes = (
            hours * 60 + minutes) * (-1 if sign == '-' else 1)

        # Get User ID
        # No user ID in this CSV file, get it from neighbouring CSV files in the same folder.
        # Open CSV files from the same folder, and try to get the 'UID' from the first element from them.
        # If the column 'UID' is not found, or the first cell is empty, pass to the next file.
        # If the 'UID' is found, break the loop and use this 'UID'.
        folderpath = os.path.dirname(filepath)
        csv_files = [file for file in os.listdir(
            folderpath) if file.endswith('.csv')]
        userId = ''
        for file in csv_files:
            if file == os.path.basename(filepath):
                continue
            try:
                tempTransactions = pd.read_csv(os.path.join(folderpath, file))
                userId = tempTransactions['UID'][0]
                break
            except KeyError:
                pass
            except IndexError:
                pass
        if userId == '':
            raise Exception(
                f"No User ID could be found in the folder '{folderpath}'. Please check the CSV files.")

        for idx, row in inTransactions.iterrows():
            try:
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(row[time_column_name]).replace(tzinfo=timezone(
                        timedelta(minutes=timezone_offset_minutes))).astimezone(timezone.utc),
                    asset=row['Currency (Fiat)'],
                    amount=row['Fiat Amount'],
                    type=TransactionType.DEPOSIT,
                    exchange=cls.name,
                    userId=userId,
                    wallet=WalletType.SPOT,  # Default transaction are done with the Spot wallet
                    note=f"Deposit Method={row['Deposit Method']}"
                ))
                # If the transaction fee is not 0, add a new transaction for the fee
                if (row['Fee'] != 0):
                    transactions.append(dataclasses.replace(transactions[-1],
                                                            amount=row['Fee'],
                                                            type=TransactionType.FEE,
                                                            note=transactions[-1].note + ', Fee'))

            except KeyError as e:
                print(f"The transaction type {
                      e} is not supported by the loader")
                exceptions_occurred = True

        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions

    @classmethod
    def load_fiat_withdrawal(cls, inTransactions, filepath) -> list[Transaction]:
        transactions = []
        exceptions_occurred = False

        # Get time column name and timezone offset
        # Get the name of the column which has the format 'Time(UTC+02:00)'
        time_column_name = [
            col for col in inTransactions.columns if 'Time(UTC' in col][0]

        # Extract the sign, hours, and minutes from the timezone string
        sign = time_column_name[8]
        hours = int(time_column_name[9:11])
        minutes = int(time_column_name[12:14])
        # Calculate the total offset in minutes
        timezone_offset_minutes = (
            hours * 60 + minutes) * (-1 if sign == '-' else 1)

        # Get User ID
        # No user ID in this CSV file, get it from neighbouring CSV files in the same folder.
        # Open CSV files from the same folder, and try to get the 'UID' from the first element from them.
        # If the column 'UID' is not found, or the first cell is empty, pass to the next file.
        # If the 'UID' is found, break the loop and use this 'UID'.
        folderpath = os.path.dirname(filepath)
        csv_files = [file for file in os.listdir(
            folderpath) if file.endswith('.csv')]
        userId = ''
        for file in csv_files:
            if file == os.path.basename(filepath):
                continue
            try:
                tempTransactions = pd.read_csv(os.path.join(folderpath, file))
                userId = tempTransactions['UID'][0]
                break
            except KeyError:
                pass
            except IndexError:
                pass
        if userId == '':
            raise Exception(
                f"No User ID could be found in the folder '{folderpath}'. Please check the CSV files.")

        for idx, row in inTransactions.iterrows():
            try:
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(row[time_column_name]).replace(tzinfo=timezone(
                        timedelta(minutes=timezone_offset_minutes))).astimezone(timezone.utc),
                    asset=row['Currency (Fiat)'],
                    amount=-row['Fiat Amount'],
                    type=TransactionType.WITHDRAW,
                    exchange=cls.name,
                    userId=userId,
                    wallet=WalletType.SPOT,  # Default transaction are done with the Spot wallet
                    note=f"Remarks={row['Withdrawal Method']}"
                ))
                # If the transaction fee is not 0, add a new transaction for the fee
                if (row['Fee'] != 0):
                    transactions.append(dataclasses.replace(transactions[-1],
                                                            amount=row['Fee'],
                                                            asset=row['Fee Currency'],
                                                            type=TransactionType.FEE,
                                                            note=transactions[-1].note + ', Fee'))

            except KeyError as e:
                print(f"The transaction type {
                      e} is not supported by the loader")
                exceptions_occurred = True

        if exceptions_occurred:
            raise Exception(
                "Exceptions occurred during the loading of the transactions. See the logs for more details.")

        return transactions
