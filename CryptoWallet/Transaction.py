from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import numpy as np

class MyEnum(Enum):
    def __str__(self):
        return self.name
    
class TransactionType(MyEnum):
    SPOT_TRADE = "Spot trade"
    STAKING_PURCHASE = "Staking purchase"
    STAKING_REDEMPTION = "Staking redemption"
    STAKING_INTEREST = "Staking interest"
    SAVING_PURCHASE = "Saving purchase"
    SAVING_REDEMPTION = "Saving redemption"
    SAVING_INTEREST = "Saving interest"
    DISTRIBUTION = "Distribution"
    DEPOSIT = "Deposit"
    FEE = "Fee"
    WITHDRAW = "Withdraw"
    REFERRAL_INTEREST = "Referral interest"
    MINING_INTEREST = "Mining interest"
    LOST = "Lost"
    STOLEN = "Stolen"
    SPEND = "Spend"
    INCOME = "Income"

class WalletType(MyEnum):
    SPOT = "Spot"
    SAVING = "Saving"
    STAKING = "Staking"
    
@dataclass
class Transaction(object):
    datetime: datetime
    asset: str
    ammount: float
    type: TransactionType
    exchange: str
    userId: str
    wallet: WalletType
    note: str = ""
    price_USD : float = np.nan
    ammount_USD: float = np.nan