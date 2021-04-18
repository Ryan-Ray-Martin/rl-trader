import ray
from ray import serve
from ppo_stock_backend import StockTradingModel

ray.init(address="auto", ignore_reinit_error=True)

client = serve.start(detached=True)

client.create_backend("ppo_stock_backend", StockTradingModel)
client.create_endpoint("ppo_endpoint", backend="ppo_stock_backend", route="/trade_stocks")