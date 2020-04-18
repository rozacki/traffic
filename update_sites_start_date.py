import json
import requests
import argparse
from datetime import datetime, timedelta
import os

from common import get_logger, valid_date

endpoint = 'http://webtris.highwaysengland.co.uk/api/v1.0/reports/daily?sites={site_id}' \
           '&start_date={start_day:02}{start_month:02}{start_year}' \
           '&end_date={stop_day:02}{stop_month:02}{stop_year}&page=1&page_size=96'


logger = get_logger()

# 400 wrong format or date too old
# 204 date is missing
# 200 date fetched

base_site_data_folder = 'data/sites'


def load_sites_info(file_name):
    with open(os.path.join(base_site_data_folder, file_name)) as f:
        return json.load(f)


def find_site_start_year(site_id, start_year):
    '''
    Deprecated
    :param site_id:
    :param start_year:
    :return:
    '''
    for year in range(start_year, datetime.now().year):
        url = endpoint.format(site_id=site_id, year=year)
        res = requests.get(endpoint.format(site_id=site_id, year=year))
        logger.info(f'{site_id}/{year} {res.status_code}')


def get_site_json_measure(site_id, startdate):
    delta_day = timedelta(days=1)
    enddate = startdate + delta_day

    url = endpoint.format(site_id=site_id, start_year=startdate.year, start_month=startdate.month,
                          start_day=startdate.day,
                          stop_year=enddate.year, stop_month=enddate.month, stop_day=enddate.day)
    res = requests.get(url)
    logger.debug(f'{url}')
    logger.info(f'{site_id}/{startdate.year}/{startdate.month}/{startdate.day} {res.status_code}')
    if res.status_code == 200:
        return res.json()
    return  None


def _update_site_json_measures_with_site_details(site_json_measures, site_dict):
    for site_json_measure in site_json_measures:
        site_json_measure.update(site_dict)
    return site_json_measures


def store_site_json(site_json_measures, site_dict, date):
    if not site_json_measures:
        return
    folder = os.path.join(base_site_data_folder,f'{site_dict["Id"]}/{date.year}/{date.month}/')
    os.makedirs(folder, exist_ok=True)
    file_name = f'{site_dict["Id"]}_{date.year}_{date.month}_{date.day}.json'
    with open(os.path.join(folder, file_name), mode='w') as f:
        site_json_measures = _update_site_json_measures_with_site_details(site_json_measures['Rows'], site_dict)
        for measure in site_json_measures:
            f.write(json.dumps(measure))
            f.write(str('\n'))


def download_and_store_site_jsons(site_dict, startdate, enddate):
    delta_day = timedelta(days=1)

    while startdate < enddate:
        site_json_measure = get_site_json_measure(site_dict['Id'], startdate)
        store_site_json(site_json_measure, site_dict, startdate)
        startdate = startdate + delta_day
    logger.info('finished')


def download_sites_jsons(file_name, startdate, enddate):
    '''
    Loads sites.json and for each site id downloads all json starting from year, month and day
    :param file_name:
    :return:
    '''
    sites_json = load_sites_info(file_name)
    logger.info(f'sites {sites_json["row_count"]}')
    for site_dict in sites_json['sites']:
        download_and_store_site_jsons(site_dict, startdate, enddate)


def download_site_jsons(file_name, site_id, startdate, enddate):
    sites_json = load_sites_info(file_name)
    logger.info(f'sites {sites_json["row_count"]}')
    for site_dict in sites_json['sites']:
        if int(site_dict['Id']) == site_id:
            logger.info(f'site {site_id} found')
            download_and_store_site_jsons(site_dict, startdate, enddate)
            return True
    return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--site', type=int, default=None)
    parser.add_argument('--sites-file', type=str, default='sites.json')
    parser.add_argument('-s', '--startdate', help="The Start Date - format YYYY-MM-DD", type=valid_date)
    parser.add_argument('-e', '--enddate', help="The Stop Date - format YYYY-MM-DD", type=valid_date,
                        default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()

    if not args.site:
        download_sites_jsons(args.sites_file, args.startdate, args.enddate)
        exit(0)
    download_site_jsons(args.sites_file, args.site, args.startdate, args.enddate)
