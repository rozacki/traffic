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


environment = setup_environment()
logging.info(f'new environment is {environment}')

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import variable

from maos.pipeline import download_road_reports, ingest

args = {'owner': 'chris'}
dag = DAG(dag_id='download_and_ingest', description='download and ingest highways england daily report',
          start_date=days_ago(-1), default_args=args)

road = variable.get_variable('road name')
startdate = variable.get_variable('start date')
enddate = variable.get_variable('end date')
datasource = variable.get_variable('data source')
overwrite = variable.get_variable('overwrite')

download = PythonOperator(dag=dag, task_id=f'download_road_{road} ({startdate};{enddate})',
                          python_callable=download_road_reports,
                          op_kwargs={'road':road, 'startdate':startdate, 'enddate': enddate})

ingest = PythonOperator(dag=dag, task_id=f'ingest_road_{road} ({startdate};{enddate}) to {datasource}',
                        python_callable=ingest,
                        op_kwargs={'datasource': datasource, 'source_folder': 'data/sites', 'overwrite': overwrite})

download >> ingest