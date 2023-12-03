import pandas as pd
from datetime import datetime
from .Transaction import Transaction, TransactionType, WalletType

class BinanceLoader:
    name = 'Binance'
    TransactionTypesMap = {
        'Deposit' : TransactionType.DEPOSIT,
        'Withdraw' : TransactionType.WITHDRAW,
        'Buy' : TransactionType.SPOT_TRADE,
        'Sell' : TransactionType.SPOT_TRADE,
        'Transaction Related' : TransactionType.SPOT_TRADE,
        'Small assets exchange BNB' : TransactionType.SPOT_TRADE,
        'Fee' : TransactionType.FEE,
        'Simple Earn Flexible Interest' : TransactionType.SAVING_INTEREST,
        'Simple Earn Flexible Subscription' : TransactionType.SAVING_PURCHASE,
        'Simple Earn Flexible Redemption' : TransactionType.SAVING_REDEMPTION,
        'Simple Earn Locked Rewards' : TransactionType.SAVING_INTEREST,
        'Rewards Distribution' : TransactionType.SAVING_INTEREST,
        'Staking Purchase' : TransactionType.STAKING_PURCHASE,
        'Staking Rewards' : TransactionType.STAKING_INTEREST,
        'Staking Redemption' : TransactionType.STAKING_REDEMPTION,
        'ETH 2.0 Staking' : TransactionType.STAKING_PURCHASE,
        'ETH 2.0 Staking Rewards' : TransactionType.STAKING_INTEREST,
        'Distribution' : TransactionType.DISTRIBUTION,
        'Cash Voucher distribution' : TransactionType.DISTRIBUTION,
        'Commission Fee Shared With You' : TransactionType.REFERRAL_INTEREST,
        'Referral Kickback' : TransactionType.REFERRAL_INTEREST
    }
    CryptoNameMap = {
        'IOTA' : 'MIOTA',
        'SHIB2': 'SHIB'
    }        
    
    @classmethod
    def load(cls, filepath_or_buffer) -> pd.DataFrame:
        inTransactions = pd.read_csv(filepath_or_buffer)
        transactions = []
        for idx, row in inTransactions.iterrows():
            transactions.append(Transaction(
                datetime=datetime.fromisoformat(row['UTC_Time']),
                asset=row['Coin'],
                ammount=row['Change'],
                type=cls.TransactionTypesMap[row['Operation']],
                exchange=cls.name,
                userId=row['User_ID'],
                wallet=WalletType.SPOT, # Default transaction are done with the Spot wallet
                note=row['Operation'] + ('' if row.isna()['Remark']  else (', ' + str(row['Remark'])))
            ))
            
            # Coins in the Saving wallet are prefixed by LD. Put them in the saving wallet and remove the prefix.
            if(transactions[-1].type in {TransactionType.SAVING_PURCHASE, TransactionType.SAVING_REDEMPTION}
            and transactions[-1].asset.startswith('LD')):
                transactions[-1].wallet = WalletType.SAVING
                transactions[-1].note += ', Original asset is ' + transactions[-1].asset
                transactions[-1].asset = transactions[-1].asset[2:]
                
            # The BETH coin is the coin representing ETH coins staked in the ETH 2.0 Staking program.
            # Put them in the Staking wallet and remove the prefix.
            # Reward of this program are given in BETH, so are directly staked and stay in the STAKING wallet.
            if(transactions[-1].asset == 'BETH'):
                transactions[-1].wallet = WalletType.STAKING
                transactions[-1].note += ', Original asset is BETH'
                transactions[-1].asset = 'ETH'
                            
            # In binance the STAKING wallet does not belong to the user. 
            # So during a staking purchase or redemption, there is a transaction telling the in/out flow of the SPOT wallet, 
            # but it does not say the in/out flow of the STAKING wallet. 
            # This transaction is therefore added by this program. 
            # Therefor Binance store the ETH 2.0 Staking in the SPOT wallet with the BETH coin. Don't create a new transaction
            # for ETH 2.0 Staking transactions
            if(transactions[-1].type in {TransactionType.STAKING_PURCHASE, TransactionType.STAKING_REDEMPTION}
            and row['Operation'] != 'ETH 2.0 Staking'):           
                transactions.append(transactions[-1])
                transactions[-1].wallet = WalletType.STAKING
                transactions[-1].ammount *=-1
                transactions[-1].note += ', Transaction not from Binance'
        
        transactions_df = pd.DataFrame(transactions)
        transactions_df['asset'] = transactions_df['asset'].map(lambda s: cls.CryptoNameMap[s] if s in cls.CryptoNameMap else s)
        
        return transactions_df