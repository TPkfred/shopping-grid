
import yaml

dow_list = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
dow_dict = dict(zip(range(7), dow_list))


class Configs:
    def __init__(self):
        self.global_configs = self.load_config_file("./global-configs.yaml")
        self.market_configs = self.load_config_file("./market-configs.yaml")

    def load_config_file(self, filename):
        with open(filename, "rb") as f:
            config = yaml.safe_load(f)
        return config

    def get_market_config(self, market):
        return self.market_configs[market]
    