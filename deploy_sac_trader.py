import ray
from ray import serve
from sac_stock_backend import StockTradingModel

ray.init(address="auto", ignore_reinit_error=True)

client = serve.start(detached=True)

client.create_backend("sac_stock_backend", StockTradingModel)
client.create_endpoint("sac_endpoint", backend="sac_stock_backend", route="/trade_stocks")