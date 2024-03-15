import pandas as pd
from datetime import datetime
from .Transaction import Transaction, TransactionType, WalletType
import dataclasses

class BinanceLoader:
    name = 'Binance'
    TransactionTypesMap = {
        'Deposit' : TransactionType.DEPOSIT,
        'Withdraw' : TransactionType.WITHDRAW,
        'Buy' : TransactionType.SPOT_TRADE,
        'Transaction Buy' : TransactionType.SPOT_TRADE,
        'Transaction Revenue' : TransactionType.SPOT_TRADE,
        'Sell' : TransactionType.SPOT_TRADE,
        'Transaction Sold' : TransactionType.SPOT_TRADE,
        'Transaction Spend' : TransactionType.SPOT_TRADE,
        'Transaction Related' : TransactionType.SPOT_TRADE,
        'Small assets exchange BNB' : TransactionType.SPOT_TRADE,
        'Small Assets Exchange BNB' : TransactionType.SPOT_TRADE,
        'Fee' : TransactionType.FEE,
        'Transaction Fee' : TransactionType.FEE,
        'Simple Earn Flexible Interest' : TransactionType.SAVING_INTEREST,
        'Simple Earn Flexible Subscription' : TransactionType.SAVING_PURCHASE,
        'Simple Earn Flexible Redemption' : TransactionType.SAVING_REDEMPTION,
        'Simple Earn Locked Rewards' : TransactionType.SAVING_INTEREST,
        'Rewards Distribution' : TransactionType.SAVING_INTEREST,
        'Simple Earn Flexible Airdrop' : TransactionType.SAVING_INTEREST,
        'Staking Purchase' : TransactionType.STAKING_PURCHASE,
        'Staking Rewards' : TransactionType.STAKING_INTEREST,
        'Staking Redemption' : TransactionType.STAKING_REDEMPTION,
        'ETH 2.0 Staking' : TransactionType.STAKING_PURCHASE,
        'ETH 2.0 Staking Rewards' : TransactionType.STAKING_INTEREST,
        'ETH 2.0 Staking Withdrawals' : TransactionType.STAKING_REDEMPTION,
        'Distribution' : TransactionType.DISTRIBUTION,
        'Airdrop Assets' : TransactionType.DISTRIBUTION,
        'Cash Voucher distribution' : TransactionType.DISTRIBUTION,
        'Cash Voucher Distribution' : TransactionType.DISTRIBUTION,
        'Commission Fee Shared With You' : TransactionType.REFERRAL_INTEREST,
        'Referral Commission' : TransactionType.REFERRAL_INTEREST,
        'Referral Kickback' : TransactionType.REFERRAL_INTEREST,
        'Token Swap - Redenomination/Rebranding' : TransactionType.REDENOMINATION,
        'Crypto Box' : TransactionType.DISTRIBUTION,
        'Binance Convert' : TransactionType.SPOT_TRADE
    }
    CryptoNameMap = {
        'SHIB2': 'SHIB'
    }        
    
    @classmethod
    def load(cls, filepath_or_buffer) -> pd.DataFrame:
        print(f"Loading transactions from {filepath_or_buffer} file")
        # Check that the file is a csv file
        if(not filepath_or_buffer.endswith('.csv')):
            raise Exception(f"The file {filepath_or_buffer} is not a csv file")
        inTransactions = pd.read_csv(filepath_or_buffer)
        transactions = []
        exceptions_occurred = False
        for idx, row in inTransactions.iterrows():
            try:
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(row['UTC_Time']),
                    asset=row['Coin'],
                    amount=row['Change'],
                    type=cls.TransactionTypesMap[row['Operation']],
                    exchange=cls.name,
                    userId=row['User_ID'],
                    wallet=WalletType.SPOT, # Default transaction are done with the Spot wallet
                    note=f"Operation={row['Operation']}" + ('' if row.isna()['Remark']  else (f", Remark={str(row['Remark'])}"))
                ))
            except KeyError as e:
                print(f"The transaction type {e} is not supported by the loader")
                exceptions_occurred = True
            
            # The BETH coin is the coin representing ETH coins staked in the ETH 2.0 Staking program.
            # Put them in the Staking wallet and remove the prefix.
            # Reward of this program are given in BETH, so are directly staked and stay in the STAKING wallet.
            if(transactions[-1].asset == 'BETH'):
                transactions[-1].wallet = WalletType.STAKING
                transactions[-1].note += ', Original asset is BETH'
                transactions[-1].asset = 'ETH'

            # In binance the SAVING wallet does not belong to the user. 
            # So during a staking purchase or redemption, there is a transaction telling the in/out flow of the SPOT wallet, 
            # but it does not say the in/out flow of the STAKING wallet. 
            # This transaction is therefore added by this program.              
            if(transactions[-1].type in {TransactionType.SAVING_PURCHASE, TransactionType.SAVING_REDEMPTION}):           
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
            if(transactions[-1].type in {TransactionType.STAKING_PURCHASE, TransactionType.STAKING_REDEMPTION}
            and row['Operation'] not in {'ETH 2.0 Staking', 'ETH 2.0 Staking Withdrawals'}):           
                transactions.append(dataclasses.replace(transactions[-1], 
                    wallet=WalletType.STAKING, 
                    amount=-transactions[-1].amount, 
                    note=transactions[-1].note + ', Transaction not from Binance'))
        
        transactions_df = pd.DataFrame(transactions)
        transactions_df['asset'] = transactions_df['asset'].map(lambda s: cls.CryptoNameMap[s] if s in cls.CryptoNameMap else s)

        if exceptions_occurred:
            raise Exception("Exceptions occurred during the loading of the transactions. See the logs for more details.")
        
        return transactions_df
      
      
      
      
      
class SwissborgLoader:
    name = 'Swissborg'
    TransactionTypesMap = {
        'Deposit' : TransactionType.DEPOSIT,
        'Withdraw' : TransactionType.WITHDRAW,
        'Buy' : TransactionType.SPOT_TRADE,
        'Sell' : TransactionType.SPOT_TRADE,
        'Payouts' : TransactionType.STAKING_INTEREST
    }     
    
    @classmethod
    def load(cls, filepath_or_buffer) -> pd.DataFrame:
        print(f"Loading transactions from {filepath_or_buffer} file")
        # Check that the file is a xlsx file
        if(not filepath_or_buffer.endswith('.xlsx')):
            raise Exception(f"The file {filepath_or_buffer} is not a xlsx file")
          
        inTransactions = pd.read_excel(filepath_or_buffer, header=13, usecols='A:K')
        # The user id is in the cell 6E row of the file
        userId = pd.read_excel(filepath_or_buffer, usecols="E", skiprows=4, nrows=1).iat[0, 0]
        
        transactions = []
        exceptions_occurred = False
        for idx, row in inTransactions.iterrows():
            try:
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(row['Time in UTC']),
                    asset=row['Currency'],
                    amount=row['Gross amount'],
                    type=cls.TransactionTypesMap[row['Type']],
                    exchange=cls.name,
                    userId=userId,
                    wallet=WalletType.SPOT, # Default transaction are done with the Spot wallet
                    note=f"Type={row['Type']}" + ('' if row.isna()['Note']  else (f", Note={str(row['Note'])}")),
                    price_USD=row['Gross amount (USD)']/row['Gross amount'],
                    amount_USD=row['Gross amount (USD)']
                ))
                # If the transaction fee is not 0, add a new transaction for the fee
                if(row['Fee'] != 0):
                    transactions.append(dataclasses.replace(transactions[-1], 
                        amount=row['Fee'], 
                        type=TransactionType.FEE, 
                        note=transactions[-1].note + ', Fee',
                        amount_USD=row['Fee (USD)']))
                
            except KeyError as e:
                print(f"The transaction type {e} is not supported by the loader")
                exceptions_occurred = True
            
        transactions_df = pd.DataFrame(transactions)

        if exceptions_occurred:
            raise Exception("Exceptions occurred during the loading of the transactions. See the logs for more details.")
        
        return transactions_df