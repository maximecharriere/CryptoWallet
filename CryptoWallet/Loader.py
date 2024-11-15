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
                                                            amount=-row['Fee'],
                                                            type=TransactionType.FEE,
                                                            note=transactions[-1].note +
                                                            ', Fee',
                                                            amount_USD=row['Fee (USD)']))

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
            'Transfer': TransactionType.TBD, # To Be Determined
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
                    
                    transactions.append(Transaction(
                        datetime=datetime.fromisoformat(str(row[time_column_name])).replace(tzinfo=timezone(
                            timedelta(minutes=timezone_offset_minutes))).astimezone(timezone.utc),
                        asset=row['Currency'],
                        amount=amount,
                        type=cls.TransactionTypesMap[row['Type']],
                        exchange=cls.name,
                        userId=row['UID'],
                        wallet=walletType,
                        note=f"Remark={row['Remark']}, Type={row['Type']}, Side={row['Side']}"
                    ))
                    
                    if (transactions[-1].type == TransactionType.TBD):
                        # if the type is a transfer between two internal account, set the type to DEPOSIT or WITHDRAW
                        if (row['Type'] == 'Transfer'):
                            transactions[-1].type = cls.TransactionTypesMap[row['Side']]
                        elif (row['Type'] == 'KuCoin Event'):
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

