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