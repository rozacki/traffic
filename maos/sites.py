import json
import os

import pandas as pd

from maos.common import configs_base_folder, logger, get_configs_base_folder


def load_sites_info(file_name='sites.json'):
    '''
    :param file_name:
    :return: dictionary
    '''
    with open(os.path.join(configs_base_folder, file_name)) as f:
        return json.load(f)


def get_sites_info(site_start=1, sites_count=1):
    '''
    return of sites_count dictionary starting from site_start position
    :param site_start:
    :param sites_count:
    :return: dictionary
    '''
    sites = load_sites_info(file_name='sites.json')
    sites = {int(i['Id']): i for i in sites['sites']}
    sites_to_return = {}
    for key, val in sites.items():
        # seek for index
        if key < site_start:
            continue
        # check if reached sites count
        if list(sites.keys()).index(key)+1 == site_start + sites_count:
            break
        sites_to_return[key] = val
    return sites_to_return


def get_road_sites(sites_file, road_name):
    '''
    :param road_name:
    :return: dictionary where key is Id
    '''
    logger.info(f'configs_base_folder={get_configs_base_folder()}')
    sites = pd.read_csv(os.path.join(get_configs_base_folder(), sites_file))
    logger.info(f'{sites_file} open')
    sites.set_index('road', drop=False, inplace=True)
    is_road_name = sites['road'] == road_name
    sites_ret = sites[is_road_name]
    sites_ret.set_index('Id', drop=False, inplace=True)
    return sites_ret.to_dict(orient='index')


def get_sites(sites_file, site_start, sites_count):
    '''
    return range of sites starting from site_start
    :param sites_file:
    :param site_start:
    :param sites_count:
    :return: dictionary where key is Id
    '''
    sites = pd.read_csv(os.path.join(configs_base_folder, sites_file))
    sites.set_index('Id', drop=False, inplace=True)
    return sites[sites['Id'] >= site_start][:sites_count].to_dict(orient='index')


def get_link_sites(sites_file, link_id):
    '''
    return all sites in the link
    :param sites_file:
    :param link_id:
    :return:
    '''
    sites = pd.read_csv(os.path.join(configs_base_folder, sites_file))
    sites.set_index('Id', drop=False, inplace=True)
    link_sites_mask = sites['Name'].str.contains(link_id, na=False)
    return sites[link_sites_mask].to_dict(orient='index')