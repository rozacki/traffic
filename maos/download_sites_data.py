import json
import requests
import argparse
from datetime import datetime, timedelta
import os
from maos.common import logger

from maos.common import valid_date, get_sites_data_folder, get_roads_data_folder, fromisoformat
from maos.sites import get_road_sites, get_sites


endpoint = 'http://webtris.highwaysengland.co.uk/api/v1.0/reports/daily?sites={site_id}' \
           '&start_date={start_day:02}{start_month:02}{start_year}' \
           '&end_date={stop_day:02}{stop_month:02}{stop_year}&page=1&page_size=96'


# 400 wrong format or date too old
# 204 date is missing
# 200 date fetched


def download_site_daily_report(site_id, startdate):
    delta_day = timedelta(days=1)
    enddate = startdate + delta_day

    url = endpoint.format(site_id=site_id, start_year=startdate.year, start_month=startdate.month,
                          start_day=startdate.day,
                          stop_year=enddate.year, stop_month=enddate.month, stop_day=enddate.day)
    res = requests.get(url)
    logger.debug(f'{url}')
    logger.debug(f'downloading daily report for site {site_id} date {startdate}'
                f'{res.status_code}')
    if res.status_code == 200:
        return res.json()
    logger.debug(f'missing daily report for site {site_id} date {startdate}')
    return  None


def _update_site_daily_report(site_daily_report, site_dict):
    '''
    Add new column which is concat od date and time, for druid to have the main __time column
    Update all rows in the report with columns that come in site_dict
    :param site_daily_report:
    :param site_dict:
    :return:
    '''
    for quarter_report in site_daily_report:
        t = fromisoformat(quarter_report['Time Period Ending'])
        d = fromisoformat(quarter_report['Report Date'])
        d = d.replace(hour=t.hour, minute=t.minute)
        site_dict['Report Date Time'] = d.isoformat()
        quarter_report.update(site_dict)
    return site_daily_report


def store_site_daily_report(site_daily_report, site_dict, date):
    if not site_daily_report:
        return
    folder = os.path.join(get_sites_data_folder(), str(site_dict["Id"]), str(date.year), str(date.month))
    file_name = f'{site_dict["Id"]}_{date.year}_{date.month}_{date.day}.json'
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, file_name), mode='w') as f:
        site_daily_report = _update_site_daily_report(site_daily_report['Rows'], site_dict)
        for measure in site_daily_report:
            json.dump(measure, f)
            f.write('\n')
    logger.info(f'daily report stored here {folder}/{file_name}')


def download_site_lazily(site_dict, startdate, enddate):
    delta_day = timedelta(days=1)
    logger.debug(f'start loading {site_dict["Id"]} from {startdate} till {enddate}')
    while startdate < enddate:
        yield startdate, download_site_daily_report(site_dict['Id'], startdate)
        startdate = startdate + delta_day


def download_and_store_reports(sites, startdate, enddate):
    logger.debug(f'start loading {len(sites)} sites from {startdate} till {enddate}')
    for key, site_dict in sites.items():
        reports = download_site_lazily(site_dict, startdate, enddate)
        for report_date, site_daily_report in reports:
            store_site_daily_report(site_daily_report, site_dict, report_date)
    logger.debug(f'finished loading and storing {len(sites)}')


def download_sites_daily_reports(sites_file, startdate, enddate, site_start, sites_count):
    '''
    :param sites_file:
    :param startdate:
    :param enddate:
    :param sites_count:
    :param site_start:
    :return:
    '''
    sites = get_sites(sites_file, site_start, sites_count)
    download_and_store_reports(sites, startdate, enddate)


def download_road_daily_reports(sites_file, road_name, startdate, enddate):
    '''
    Finds all sites associated with provided road, downloads all json between startdate and enddate
    :param sites_file:
    :param road_name:
    :param startdate:
    :param enddate:
    :return:
    '''
    sites = get_road_sites(sites_file, road_name)
    logger.debug(f'start loading {len(sites)} sites for road {road_name}')
    download_and_store_reports(sites, startdate, enddate)
    logger.debug(f'finished loading ans storing {len(sites)} sites for road {road_name}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--site', type=int, default=1, help='Seek and start downloading from this site')
    parser.add_argument('--sites-count', type=int, default=1, help='How many sites try to download. '
                                                                     'Note some data may no be available for site')
    parser.add_argument('--sites-catalog', type=str, default='sites_catalog.csv')
    parser.add_argument('-s', '--startdate', help="The Start Date - format YYYY-MM-DD", type=valid_date)
    parser.add_argument('-e', '--enddate', help="The Stop Date - format YYYY-MM-DD", type=valid_date,
                        default=datetime.now().strftime('%Y-%m-%d'))
    parser.add_argument('--road', type=str)
    args = parser.parse_args()

    if args.road:
        download_road_daily_reports(args.sites_file, args.road, args.startdate, args.enddate)
        exit(0)

    # loads data for all sites in sites_file
    download_sites_daily_reports(args.sites_file, args.startdate, args.enddate, args.site, args.sites_count)
