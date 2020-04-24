import logging
import logging.config
from datetime import datetime
import argparse
import os

configs_base_folder='configs'

def get_logger(name='main'):
    logging.config.fileConfig(os.path.join(configs_base_folder, 'logging.conf'))
    return logging.getLogger(name)


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)