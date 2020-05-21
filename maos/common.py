import logging
import logging.config
from datetime import datetime
import argparse
import dateutil.parser


config_folder = 'configs'
sites_data_folder = 'data/sites'
roads_data_folder = 'data/roads'
scripts_folder = 'scripts'


def get_logger(name='main'):
    #logging.config.fileConfig(os.path.join(configs_base_folder, 'logging.conf'))
    return logging.getLogger(name)


logger = get_logger()


def set_globals(new_config_folder, new_site_data_folder, new_scripts_folder):
    global config_folder, sites_data_folder
    config_folder = new_config_folder
    sites_data_folder = new_site_data_folder
    scripts_folder = new_scripts_folder
    logger.info(f'configs_base_folder={config_folder}')
    logger.info(f'base_site_data_folder={sites_data_folder}')


def get_config_folder():
    return config_folder


def get_sites_data_folder():
    return sites_data_folder


def get_roads_data_folder():
    return roads_data_folder


def get_scripts_folder():
    return scripts_folder

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
