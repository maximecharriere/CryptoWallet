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
        'Distribution' : TransactionType.DISTRIBUTION,
        'Airdrop Assets' : TransactionType.DISTRIBUTION,
        'Cash Voucher distribution' : TransactionType.DISTRIBUTION,
        'Cash Voucher Distribution' : TransactionType.DISTRIBUTION,
        'Commission Fee Shared With You' : TransactionType.REFERRAL_INTEREST,
        'Referral Commission' : TransactionType.REFERRAL_INTEREST,
        'Referral Kickback' : TransactionType.REFERRAL_INTEREST,
        'Token Swap - Redenomination/Rebranding' : TransactionType.REDENOMINATION
    }
    CryptoNameMap = {
        'IOTA' : 'MIOTA',
        'SHIB2': 'SHIB'
    }        
    
    @classmethod
    def load(cls, filepath_or_buffer) -> pd.DataFrame:
        inTransactions = pd.read_csv(filepath_or_buffer)
        transactions = []
        exceptions_occurred = False
        for idx, row in inTransactions.iterrows():
            try:
                transactions.append(Transaction(
                    datetime=datetime.fromisoformat(row['UTC_Time']),
                    asset=row['Coin'],
                    ammount=row['Change'],
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
                    ammount=-transactions[-1].ammount, 
                    note=transactions[-1].note + ', Transaction not from Binance'))
            
            # In binance the STAKING wallet does not belong to the user. 
            # So during a staking purchase or redemption, there is a transaction telling the in/out flow of the SPOT wallet, 
            # but it does not say the in/out flow of the STAKING wallet. 
            # This transaction is therefore added by this program. 
            # Therefor Binance store the ETH 2.0 Staking in the SPOT wallet with the BETH coin. Don't create a new transaction
            # for ETH 2.0 Staking transactions
            if(transactions[-1].type in {TransactionType.STAKING_PURCHASE, TransactionType.STAKING_REDEMPTION}
            and row['Operation'] != 'ETH 2.0 Staking'):           
                transactions.append(dataclasses.replace(transactions[-1], 
                    wallet=WalletType.STAKING, 
                    ammount=-transactions[-1].ammount, 
                    note=transactions[-1].note + ', Transaction not from Binance'))
        
        transactions_df = pd.DataFrame(transactions)
        transactions_df['asset'] = transactions_df['asset'].map(lambda s: cls.CryptoNameMap[s] if s in cls.CryptoNameMap else s)

        if exceptions_occurred:
            raise Exception("Exceptions occurred during the loading of the transactions. See the logs for more details.")
        
        return transactions_df