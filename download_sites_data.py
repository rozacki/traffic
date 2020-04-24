import json
import requests
import argparse
from datetime import datetime, timedelta, time
import os

from common import get_logger, valid_date, load_sites_info

endpoint = 'http://webtris.highwaysengland.co.uk/api/v1.0/reports/daily?sites={site_id}' \
           '&start_date={start_day:02}{start_month:02}{start_year}' \
           '&end_date={stop_day:02}{stop_month:02}{stop_year}&page=1&page_size=96'


logger = get_logger()

# 400 wrong format or date too old
# 204 date is missing
# 200 date fetched


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


def get_site_daily_report(site_id, startdate):
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


def _update_site_daily_report(site_daily_report, site_dict):
    for quarter_report in site_daily_report:
        t = time.fromisoformat(quarter_report['Time Period Ending'])
        d = datetime.fromisoformat(quarter_report['Report Date'])
        d = d.replace(hour=t.hour, minute=t.minute)
        site_dict['Report Date Time'] = d.isoformat()
        quarter_report.update(site_dict)
    return site_daily_report


def store_site_daily_report(site_json_daily_report, site_dict, date):
    if not site_json_daily_report:
        return
    folder = os.path.join(base_site_data_folder,f'{site_dict["Id"]}/{date.year}/{date.month}/')
    os.makedirs(folder, exist_ok=True)
    file_name = f'{site_dict["Id"]}_{date.year}_{date.month}_{date.day}.json'
    with open(os.path.join(folder, file_name), mode='w') as f:
        site_json_daily_report = _update_site_daily_report(site_json_daily_report['Rows'], site_dict)
        for measure in site_json_daily_report:
            json.dump(measure, f)
            f.write('\n')


def download_and_store_site_jsons(site_dict, startdate, enddate):
    delta_day = timedelta(days=1)

    while startdate < enddate:
        site_json_daily_report = get_site_daily_report(site_dict['Id'], startdate)
        store_site_daily_report(site_json_daily_report, site_dict, startdate)
        startdate = startdate + delta_day
    logger.info(f'finished loading site {site_dict["Id"]}')


def download_sites_daily_reports(file_name, startdate, enddate, sites_count, site_start):
    '''
    Loads sites.json and for each site id downloads all json starting from year, month and day
    :param file_name:
    :return:
    '''
    sites_json = load_sites_info(file_name)
    logger.info(f'sites {sites_json["row_count"]}')
    sites_downloaded = 0
    site_start_found = False
    for site_dict in sites_json['sites']:
        if not site_start_found and site_start != int(site_dict['Id']):
            logger.debug(f'start site {site_start} not found {site_dict["Id"]}, continuing')
            continue
        else:
            site_start_found = True

        if sites_downloaded >= sites_count:
            logger.debug(f'max number of sites {sites_count} reached')
            break
        sites_downloaded = sites_downloaded + 1
        download_and_store_site_jsons(site_dict, startdate, enddate)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--site', type=int, default=1, help='Seek and start downloading from this site')
    parser.add_argument('--sites-count', type=int, default=1, help='How many sites try to download. '
                                                                     'Note some data may no be available for site')
    parser.add_argument('--sites-file', type=str, default='sites.json')
    parser.add_argument('-s', '--startdate', help="The Start Date - format YYYY-MM-DD", type=valid_date)
    parser.add_argument('-e', '--enddate', help="The Stop Date - format YYYY-MM-DD", type=valid_date,
                        default=datetime.now().strftime('%Y-%m-%d'))
    args = parser.parse_args()

    if args.site:
        args.site_count = 1

    # loads data for all sites in sites_file
    download_sites_daily_reports(args.sites_file, args.startdate, args.enddate, args.sites_count, args.site)
