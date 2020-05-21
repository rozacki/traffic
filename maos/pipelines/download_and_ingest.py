import logging


def setup_environment():
    '''
    based on folder for example /home/chris/airflow/dags/prod/traffic
    will find environment=prod
    update sys.path to point to /home/chris/airflow/dags/prod/traffic
    :return:
    '''
    import sys, os
    # hardcoded segments
    cwd = os.path.dirname(os.path.realpath(__file__))
    # remove first empty segment
    segments = cwd.split('/')[1:]
    # join back first 5 segments as this is where repo starts
    sys_path = os.path.join('/', *segments[:6])
    # extract 4th segment as global variable
    environment = segments[4:5][0]
    sys.path.insert(0, sys_path)
    logging.info('new sys path is {}'.format(sys_path))
    ofgem_libs = [key for key in sys.modules.keys() if 'maos' in key]
    for i in ofgem_libs:
        del (sys.modules[i])
    logging.info('removed `maos` libraries')
    return environment


new_environment = setup_environment()

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import variable

from maos.pipeline import download_road_reports, ingest
from maos.common import remove_non_alnum, set_globals, valid_date

args = {'owner': 'chris'}
dag = DAG(dag_id='download_and_ingest', description='download and ingest highways england daily report',
          start_date=days_ago(1), default_args=args, schedule_interval=None)

road = variable.get_variable('road name')
startdate = valid_date(variable.get_variable('start date'))
enddate = valid_date(variable.get_variable('end date'))
datasource = variable.get_variable('data source')
append_to_existing = variable.get_variable('append_to_existing')
sites_catalog_folder = variable.get_variable('sites catalog folder')
sites_data_folder = variable.get_variable('site data catalog folder')
scripts_folder = variable.get_variable('scripts folder')

set_globals(sites_catalog_folder, sites_data_folder, scripts_folder)

download = PythonOperator(python_callable=download_road_reports,
                          dag=dag,
                          task_id=remove_non_alnum(f'download_road_{road}_({startdate}-{enddate})'),
                          op_kwargs={'road': road, 'startdate': startdate,
                                     'enddate': enddate})

ingest = PythonOperator(python_callable=ingest,
                        dag=dag,
                        task_id=remove_non_alnum(f'ingest_road_{road}_({startdate}-{enddate}) to {datasource}'),
                        op_kwargs={'datasource': datasource, 'source_folder': sites_data_folder,
                                   'overwrite': append_to_existing})

download >> ingest