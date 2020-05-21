import os
from subprocess import Popen, PIPE
import threading
import shutil
import tempfile

from maos.common import logger, set_globals
from maos.sites import get_sites, get_link_sites, get_road_sites
from maos.download_sites_data import download_and_store_reports


'''
How many threads can be run in parallel
'''
cpus = 4
threads_per_cpu = 5
max_threads = cpus*threads_per_cpu
max_threads_semaphore = threading.Semaphore(max_threads)
path_to_post_index_path_script = '/root/druid/apache-druid-0.17.0/bin/post-index-task'


def _get_thread_name(site_id, startdate, enddate):
    ''' create thread name from job details '''
    return f'site-{site_id}-from-{startdate}-to-{enddate}'


def run_process(cmd):
    '''
    runs an arbitrary process
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


def ingest(datasource, overwrite, source_folder, ingestion_template_file_name='ingestion_template.json',
           url='http://localhost:8081'):
    '''
    Ingest data from the folder
    :param datasource:
    :param source_folder:
    :param overwrite:
    :param ingestion_template_file_name:
    :param url:
    :return:
    '''
    template = os.path.join('ingestions', ingestion_template_file_name)
    logger.info(f'used {template} template to post the task')
    with open(template) as f:
        template = f.read()
    overwrite = 'true' if overwrite else "false"
    task_string = template.format(datasource_name=datasource, source_folder=source_folder,
                                  append_to_existing=overwrite)
    logger.info(task_string)
    with tempfile.NamedTemporaryFile(mode='w') as tmp:
        tmp.write(task_string)
        tmp.flush()
        logger.info(f'task definition stored in {tmp.name}')
        cmd = [path_to_post_index_path_script, '--file', tmp.name, '--url', url]
        logger.info(f'{cmd}')
        run_process(cmd)


def _wrap_download_and_store_reports(site, startdate, enddate):
    '''
    This is where thread starts. Here we control number of threads running simultaneously
    :param site:
    :param startdate:
    :param enddate:
    :return:
    '''
    current_thread = threading.current_thread()
    logger.info(f'starting thread {current_thread.getName()}')
    max_threads_semaphore.acquire()
    logger.info(f'started thread {current_thread.getName()}')
    try:
        download_and_store_reports(site, startdate, enddate)
    except Exception as ex:
        logger.error(str(ex))
    max_threads_semaphore.release()
    logger.info(f'finished thread {current_thread.getName()}')


def _download_reports_async(sites, startdate, enddate, download_folder='data/sites', maximum_folder_size=584000):
    '''
    downloads data into folder, checks if it does not overcome maximum limit
    :param sites:
    :param sites_count:
    :param startdate:
    :param cpus:
    :param threads_per_cpu:
    :param download_folder:
    :param maximum_folder_size:
    :return:
    '''
    if (enddate - startdate).days * len(sites) > maximum_folder_size:
        raise Exception('maximum size folder')
    try:
        shutil.rmtree(download_folder)
    except Exception as ex:
        logger.error(ex)
        pass
    logger.info(f'folder {download_folder} content removed')

    threads = []
    for id, site in sites.items():
        t = threading.Thread(target=_wrap_download_and_store_reports,
                             args=({id: site}, startdate, enddate, ), daemon=True)
        t.setName(_get_thread_name(id, startdate, enddate))
        t.start()
        threads.append(t)

    for key, thread in enumerate(threads):
        logger.info(f'wait for {thread.getName()} thread to finish')
        thread.join()
    return download_folder


def download_road_reports(road, startdate, enddate):
    sites = get_road_sites('sites_catalog.csv', road)
    _download_reports_async(sites, startdate, enddate)


def download_sites_reports(site, sites_count, startdate, enddate):
    sites = get_sites('sites_catalog.csv', site, sites_count)
    _download_reports_async(sites, startdate, enddate)


def download_link_reports(link, startdate, enddate):
    sites = get_link_sites('sites_catalog.csv', link)
    _download_reports_async(sites, startdate, enddate)
