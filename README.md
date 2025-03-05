# CryptoWallet

A Python-based tool to track and analyze your cryptocurrency portfolio across multiple exchanges and wallets. It helps you manage and consolidate your crypto assets, providing detailed insights into your holdings.

## Features

- **Multi-Exchange Support**: 
  - Binance
  - Ledger
  - Swissborg
  - Kucoin
  - Bybit
  - Coinbase (deprecated)
  
- **Asset Tracking**:
  - Track total holdings across all exchanges
  - Monitor spot, saving, and staking wallets
  - Calculate total value in USD
  - Track fees and interests earned
  
- **Data Analysis**:
  - Calculate potential revenue and current value
  - Generate detailed statistics per coin
  - Export data to Excel for further analysis

- **TradingView Integration**:
  - Export data to TradingView for visualization
  - Show buy/sell orders
  - Show Mean buy price

## Setup

1. Clone this repository
2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure your settings in the `.CryptoWallet/settings.json` file:
   ```json
   {
     "root_dirpath": "path/to/your/data",
     "database_filepath": "Transactions/transactions.csv",
     "exported_transactions_dirpath": "ExportedTransactions",
     "output_dirpath": "Output",
     "cryptocompare_api_key": "YOUR_API_KEY"
   }
   ```
   Get your API key from: https://www.cryptocompare.com/cryptopian/api-keys


## Usage

1. Export your transactions from supported exchanges (see documentation in the Jupyter notebook for specific instructions per exchange)

2. Place your exported transaction files in the appropriate folders under `ExportedTransactions/`:
   ```
   ExportedTransactions/
   ├── Binance/
   ├── Ledger/
   ├── SwissBorg/
   ├── Kucoin/
   ├── ByBit/
   └── Manual/
   ```

3. Open and run the `CryptoWallet.ipynb` notebook:
   - Import transactions from each exchange
   - Generate statistics and analysis
   - Export results to Excel

## Manual Transactions

For exchanges or transactions not supported by the automatic loaders, you can create a CSV file in the `ExportedTransactions/Manual/` directory with the following columns:

- datetime (ISO8601 format)
- asset
- amount
- type (see TransactionType enum)
- exchange
- userId
- wallet (see WalletType enum)
- note (optional)
- price_USD (optional)
- amount_USD (optional)

## Output

The tool generates several output files:

1. Excel file with multiple sheets:
   - Summary of total holdings and profits
   - Detailed statistics per coin
   - Wallet balances across exchanges
   - Complete transaction history

2. TradingView export file for visualization of your portfolio
