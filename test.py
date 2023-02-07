from requests_futures.sessions import FuturesSession

API_KEY = '29d2d18edb47bd2fb5d27ae36e57fe012dd4bd38b8d0b7f65f1de810b0f33f47'

session = FuturesSession()
api_url = 'https://min-api.cryptocompare.com/data/v2/histohour'
api_headers = {
    "authorization": "Apikey " + API_KEY
}
api_parameters = {
    'fsym': 'ETH',
    'tsym':'USD',
    'limit':'1',
    'toTs':1641314904,
    'extraParams':'CryptoWallet'
} 

response = session.get(api_url, headers=api_headers, params=api_parameters)