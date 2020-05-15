import os
from maos import logger
from maos.sites import load_sites_info
from common import configs_base_folder
import pandas as pd


def join_sites_and_conversions(sites_file='sites.json', conversion_file='ConversionTable.csv',
                               enriched_file='sites_enriched.csv'):
    '''
    This script merges sites and conversion table into single csv file config/sites_enriched.csv
    Data sources
    - sites:
    -- curl http://webtris.highwaysengland.co.uk/api/v1.0/sites
    - conversion tables
    -- curl http://tris.highwaysengland.co.uk/ConversionTable
    '''
    sites = pd.DataFrame(load_sites_info(sites_file)['sites'])
    sites['Id'] = sites['Id'].astype(int)
    sites.set_index('Id')

    conversion_table = pd.read_csv(os.path.join(configs_base_folder, conversion_file))
    conversion_table['MeasurementSiteID'] = conversion_table['MeasurementSiteID'].astype(int)
    conversion_table.set_index('MeasurementSiteID')
    joined = pd.merge(sites, conversion_table, how='left', left_on='Id', right_on='MeasurementSiteID')
    #joined = sites.join(conversion_table, how='left')
    joined.to_csv(os.path.join(configs_base_folder, enriched_file), index=False)
    return joined


def get_road_from_description(description):
    '''
    description = M4/2295A2
    :param description:
    :return:
    '''
    if not description:
        return  None
    segments = str(description).split('/')
    if len(segments) != 2:
        return None
    return segments[0]


def _get_legacy_kv_from_name(site_id, name):
    '''
    todo
    Name column can be different format for example
    'MIDAS site at M1/4174A, 062/7/123/211 on M1 northbound within J28 on road M1
    at location 53.099674303682000,-1.321691031373000'
    :param name:
    :return:
    '''
    return dict()


def get_columns_from_name(site_id, name):
    '''
    return key-value from name column
    Example:
    MIDAS site at M1/3942A priority 1 on link 123013701; GPS Ref: 447146;336274; Northbound
    will return
    site_type: 'MIDAS'
    link: 123013701
    gps_x: 447146
    gps_y: 336274
    :param name:
    :return:
    '''
    try:
        if not name:
            return None
        segments = name.split(';')
        kv = dict()
        split_segments = segments[0].split(' ')
        kv['site_type'] = split_segments[0]
        kv['link'] = split_segments[-1]
        gps_x_kv = segments[1].split(':')
        kv['gps_x'] = int(gps_x_kv[1])
        kv['gps_y'] = int(segments[2])
        kv['direction'] = segments[-1].strip()
        return kv
    except Exception as ex:
        logger.info(f'name format exception, site id {site_id} -- {name}')
        return dict()


def _enrich(row):
    road_name = get_road_from_description(row['Description'])
    row['road'] = road_name
    new_columns = get_columns_from_name(row['Id'], row['Name'])
    for k, v in new_columns.items():
        row[k] = v
    return row


def add_road_name_column(sites_csv, output_file_name='sites_enriched_roads.csv'):
    '''
    Add road name column to sites enriched and stores as csv
    :param sites_csv:
    :param output_file_name:
    :return:
    '''
    sites = pd.read_csv(os.path.join(configs_base_folder, sites_csv))
    sites = sites.apply(_enrich, axis=1)
    sites.to_csv(os.path.join(configs_base_folder, output_file_name), index=False)
    return sites


#add_road_name_column(sites_csv='sites_enriched_roads.csv', output_file_name='sites_enriched_roads_2.csv')
#join_sites_and_conversions()