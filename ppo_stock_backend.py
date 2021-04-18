import os
import ray
from ray import serve
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
from lib import data, environ
import numpy as np
import requests
from starlette.requests import Request
#from load_agent import LoadAgent
import collections
from collections import namedtuple
import json

Prices = collections.namedtuple('Prices', field_names=['open', 'high', 'low', 'close', 'volume'])

torch, nn = try_import_torch()

class TorchCustomModel(TorchModelV2, nn.Module):
    """Example of a PyTorch custom model that just delegates to a fc-net."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        self.torch_sub_model = TorchFC(obs_space, action_space, num_outputs,
                                       model_config, name)

    def forward(self, input_dict, state, seq_lens):
        input_dict["obs"] = input_dict["obs"].float()
        fc_out, _ = self.torch_sub_model(input_dict, state, seq_lens)
        return fc_out, []

    def value_function(self):
        return torch.reshape(self.torch_sub_model.value_function(), [-1])

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

#ray.shutdown()
#ray.init(num_cpus=16, num_gpus=0, ignore_reinit_error=True)
ModelCatalog.register_custom_model(
     "my_model", TorchCustomModel)

config_model = {
            "env": "myEnv",  
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            "model": {
                "custom_model": "my_model",
                "vf_share_layers": False,
            },
            "batch_mode": "truncate_episodes",
            "sgd_minibatch_size": 32,
            "num_sgd_iter": 10,
            "lr": 3e-3,  # try different lrs
            "num_workers": 1,  # parallelism
            "framework": "torch",
        }
config_model["num_workers"] = 0
config_model["exploration_config"] = {
            "type": "Curiosity",  # <- Use the Curiosity module for exploring.
            "eta": 1.0,  # Weight for intrinsic rewards before being added to extrinsic ones.
            "lr": .001,  # Learning rate of the curiosity (ICM) module.
            "feature_dim": 288,  # Dimensionality of the generated feature vectors.
            # Setup of the feature net (used to encode observations into feature (latent) vectors).
            "feature_net_config": {
                "fcnet_hiddens": [],
                "fcnet_activation": "relu",
            },
            "inverse_net_hiddens": [256],  # Hidden layers of the "inverse" model.
            "inverse_net_activation": "relu",  # Activation of the "inverse" model.
            "forward_net_hiddens": [256],  # Hidden layers of the "forward" model.
            "forward_net_activation": "relu",  # Activation of the "forward" model.
            "beta": 0.2,  # Weight for the "forward" loss (beta) over the "inverse" loss (1.0 - beta).
            # Specify, which exploration sub-type to use (usually, the algo's "default"
            # exploration, e.g. EpsilonGreedy for DQN, StochasticSampling for PG/SAC).
            "sub_exploration": {
                "type": "StochasticSampling",
        }
    }

class StockTradingModel:
    def __init__(self):
        self.agent = ray.rllib.agents.ppo.PPOTrainer(config=config_model, env="myEnv")
        self.agent.restore("/Users/user/Desktop/Market_Research/stock_data/ppo_model_batch30/PPO_2021-03-16_16-05-22/PPO_myEnv_16183_00000_0_2021-03-16_16-05-22/checkpoint_25/checkpoint-25")

    async def __call__ (self, request: Request):
        #print(" ---> request recieved", request)
        json_input = await request.json()
        obs = json_input["observation"]
        responder = {0:"skip", 1:"buy", 2:"close"}
        action = self.agent.compute_action(obs)
        print("action", action)
        return responder[action]
        

        
        