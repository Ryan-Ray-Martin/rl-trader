import os
import ray
from ray import serve
import ray.rllib.agents.sac as sac
from ray import tune
from ray.tune import grid_search, analysis
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.tf.fcnet import FullyConnectedNetwork
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFC
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import register_env
from ray.rllib.agents.sac.sac_tf_policy import SACTFPolicy
from lib import data, environ
import numpy as np
import requests
from starlette.requests import Request
import collections
from collections import namedtuple
import json

Prices = collections.namedtuple('Prices', field_names=['open', 'high', 'low', 'close', 'volume'])

torch, nn = try_import_torch()

def env_creator(env_name):
    if env_name == "StocksEnv-v0":
        from lib.environ import StocksEnv as env
    else:
        raise NotImplementedError
    return env

# register the env
BARS_COUNT = 30
STOCKS = '/Users/user/Desktop/Market_Research/stock_data/stock_prices__min_train_NET.csv'
stock_data = {"NIO": data.load_relative(STOCKS)}
env = env_creator("StocksEnv-v0")
tune.register_env('myEnv', lambda config: env(stock_data, bars_count=BARS_COUNT, state_1d=False))

config_model = sac.DEFAULT_CONFIG.copy()
config_model["policy_model"] = sac.DEFAULT_CONFIG["policy_model"].copy()
config_model["env"] = "myEnv" 
config_model["gamma"] = 1.0
config_model["no_done_at_end"] = True
config_model["tau"] = 3e-3
config_model["target_network_update_freq"] = 32
config_model["num_workers"] = 1  # Run locally.
config_model["twin_q"] = True
config_model["clip_actions"] = True
config_model["normalize_actions"] = True
config_model["learning_starts"] = 0
config_model["prioritized_replay"] = True
config_model["train_batch_size"] = 32
config_model["optimization"]["actor_learning_rate"] = 0.01
config_model["optimization"]["critic_learning_rate"] = 0.01
config_model["optimization"]["entropy_learning_rate"] = 0.003        

class StockTradingModel:
    def __init__(self):
        self.agent = ray.rllib.agents.sac.SACTrainer(config=config_model, env="myEnv")
        self.agent.restore("/Users/user/Desktop/Market_Research/stock_data/SAC_model_.5/SAC_myEnv_c17e4_00000_0_2021-03-27_18-20-17/checkpoint_4/checkpoint-4")

    async def __call__ (self, request: Request):
        #print(" ---> request recieved", request)
        json_input = await request.json()
        obs = json_input["observation"]
        responder = {0:"skip", 1:"buy", 2:"close"}
        action = self.agent.compute_action(obs)
        return responder[action]