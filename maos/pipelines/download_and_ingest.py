import logging

def setup_environment():
    import sys, os
    # hardcoded segments
    cwd = os.path.dirname(os.path.realpath(__file__))
    # remove first empty segment
    segments = cwd.split('/')[1:]
    # join back first 5 segments as this is where repo starts
    sys_path = os.path.join('/', *segments[:5])
    # extract 4th segment as global variable
    environment = segments[3:4][0]
    sys.path.insert(0, sys_path)
    logging.info('new sys path is {}'.format(sys_path))
    ofgem_libs = [key for key in sys.modules.keys() if 'ofgem' in key]
    for i in ofgem_libs:
        del (sys.modules[i])
    logging.info('removed `ofgem` libraries')
    return environment

environment = setup_environment()
logging.info(f'new environment is {environment}')

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import variable

from maos.pipeline import download_road_reports

args = {'owner': 'chris'}
dag = DAG(dag_id='download_and_ingest', description='download and ingest highways england daily report',
          start_date=days_ago(-1), default_args=args)

road = variable.get_variable('road')
startdate = variable.get_variable('startdate')
enddate = variable.get_variable('enddate')
PythonOperator(dag=dag, task_id=f'download_road_{road}_from_{startdate}_to_{enddate}',python_callable=download_road_reports,
               op_kwargs={'road':road, 'startdate':startdate, 'enddate': enddate})