from subprocess import Popen, PIPE
import argparse
from datetime import datetime
from common import get_logger, valid_date

logger = get_logger()


def partition_reports():
    pass


def run_process(cmd):
    '''
    Download
    :param site:
    :param startdate:
    :param enddate:
    :param site_count:
    :return:
    '''
    logger.info(f'start process {cmd}')
    try:
        rc = Popen(cmd, stdout=PIPE)
        content, err = rc.communicate()
        if err:
            logger.error(f'error {err} on {cmd}')
            return False
        logger.info(f'process {cmd} finished OK')
    except Exception as ex:
        logger.error(f'exception {ex} on {cmd}')
        return False


def download_reports_async(road_name, startdate, enddate):


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--site', type=int, default=1, help='Seek and start downloading from this site')
    parser.add_argument('--sites-count', type=int, default=1, help='How many sites try to download. '
                                                                   'Note some data may no be available for site')
    parser.add_argument('--sites-file', type=str, default='sites_enriched_roads.csv')
    parser.add_argument('-s', '--startdate', help="The Start Date - format YYYY-MM-DD", type=valid_date)
    parser.add_argument('-e', '--enddate', help="The Stop Date - format YYYY-MM-DD", type=valid_date,
                        default=datetime.now().strftime('%Y-%m-%d'))
    run_process()