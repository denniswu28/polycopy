from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY

host: str = "https://clob.polymarket.com"
key: str = "b4f6abaa9e68714d4551303ca2212e0efae43fc0736c55f9ba715349aff59984" #This is your Private Key. Export from https://reveal.magic.link/polymarket or from your Web3 Extension
chain_id: int = 137 # No need to adjust this
POLYMARKET_PROXY_ADDRESS: str = '0x329D521a98D9A52687E419dcc70201B38cDfa2Ee' #This is the address listed below your profile picture when using the Polymarket site.

# Select from the following 3 initialization options to match your login method, and remove any unused lines so only one client is initialized.

### Initialization of a client using a Polymarket Proxy associated with a Browser Wallet(Metamask, Coinbase Wallet, etc)
client = ClobClient(host, key=key, chain_id=chain_id, signature_type=2, funder=POLYMARKET_PROXY_ADDRESS)

## Create and sign a limit order buying 5 tokens for 0.010c each
#Refer to the API documentation to locate a tokenID: https://docs.polymarket.com/developers/gamma-markets-api/fetch-markets-guide

client.set_api_creds(client.create_or_derive_api_creds()) 

order_args = OrderArgs(
    price=0.1,
    size=10.0,
    side=BUY,
    token_id="110765302299513456585207845551762220756544326353398751489227277620687637943699", #Token ID you want to purchase goes here. Example token: 114304586861386186441621124384163963092522056897081085884483958561365015034812 ( Xi Jinping out in 2025, YES side )
)
signed_order = client.create_order(order_args)

## GTC(Good-Till-Cancelled) Order
resp = client.post_order(signed_order, OrderType.GTC)
print(resp)