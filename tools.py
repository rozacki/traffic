import os
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
        return
    segments = str(description).split('/')
    if len(segments) != 2:
        return
    return segments[0]


def add_road_name_column(sites_csv, output_file_name='sites_enriched_roads.csv'):
    '''
    Add road name column to sites enriched and stores as csv
    :param sites_csv:
    :param output_file_name:
    :return:
    '''
    sites = pd.read_csv(os.path.join(configs_base_folder, sites_csv))
    sites['road'] = sites.apply(lambda row: get_road_from_description(row['Description']), axis=1)
    sites.to_csv(os.path.join(configs_base_folder, output_file_name), index=False)
    return sites


#add_road_name_column('sites_enriched.csv')
#join_sites_and_conversions()