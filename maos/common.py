import logging
import logging.config
from datetime import datetime
import argparse
import dateutil.parser


configs_base_folder='configs'
base_site_data_folder = 'data/sites'
base_road_data_folder = 'data/roads'


def set_globals(new_configs_base_folder, new_base_site_data_folder):
    global configs_base_folder, base_site_data_folder
    configs_base_folder = new_configs_base_folder
    base_site_data_folder = new_base_site_data_folder


def get_logger(name='main'):
    #logging.config.fileConfig(os.path.join(configs_base_folder, 'logging.conf'))
    return logging.getLogger(name)


logger = get_logger()


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def fromisoformat(str):
    try:
        return dateutil.parser.parse(str)
    except ValueError:
        msg = f"{str} is not valid ISO format"
        raise ValueError(msg)


def remove_non_alnum(s):
    return "".join([ c if c.isalnum() else "_" for c in s ])
