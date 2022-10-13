
import logging
import datetime

dow_list = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
dow_dict = dict(zip(range(7), dow_list))

# shopping data older than this is considered too "stale" to use
stale_cutoff = 30

holidays = [ # includes common travel days
    datetime.date(2022,9,5), # Labor Day
    datetime.date(2022,11,23), # Day before Thanksgiving
    datetime.date(2022,11,24), # Thanksgiving
    datetime.date(2022,11,25), # Day after Thanksgiving
    datetime.date(2022,12,23), # Christmas Eve Obs
    datetime.date(2022,12,24), # Christmas Eve
    datetime.date(2022,12,25), # Christmas
    datetime.date(2022,12,26), # Christmas Obs
]

# class Configs:
#     def __init__(self):
#         self.global_configs = self.load_config_file("./global-configs.yaml")
#         self.market_configs = self.load_config_file("./market-configs.yaml")

#     def load_config_file(self, filename):
#         with open(filename, "rb") as f:
#             config = yaml.safe_load(f)
#         return config

#     def get_market_config(self, market):
#         return self.market_configs.get(
#             market, 
#             self.global_configs['generic_market_config']
#         )


def configure_logger(
        logger_name: str,
        log_level: int = 20, # INFO
        log_file_path=None,
        log_file_level=10 # DEBUG
    ):
    """Configures a python logger object.

    Logging messages will be sent to stdout and optionally to a file.
    A different logging level can be configured between the two 
    destinations.

    Note levels should be supplied as int's or logging.XXX level objects

    params:
    ----------
    logger_name (str): name of logger
    log_level (int): logging level for stdout. Default = 20, which is INFO.
    log_file (None or str): file path for the logging file. Default is None,
        which will not create a logging file handler. 
    log_file_level (int): logging level for file. Default = 10, which is DEBUG.

    returns:
    --------
    logging.Logger object
    """
    # logging formatting
    msg_fmt = '%(asctime)s - %(filename)s - %(message)s'
    date_fmt = '%Y-%m-%d %H:%M'

    basic_format = {'format': msg_fmt, 'datefmt': date_fmt}
    fmtr_format = {'fmt': msg_fmt, 'datefmt': date_fmt}

    logging.basicConfig(level=log_level, **basic_format)
    logger = logging.getLogger(logger_name)

    if log_file_path:
        f_handler = logging.FileHandler(log_file_path)
        f_handler.setLevel(log_file_level)
        f_handler.setFormatter(logging.Formatter(**fmtr_format))
        logger.addHandler(f_handler)

    return logger