# main DAG that runs daily


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum

from scripts.extract_gdelt import extract_gdelt_conflict_events
from scripts.extract_prices import extract_commodity_prices
from scripts.extract_disasters import extract_disaster_events


default_args = {
    'owner': 'shocktrace',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}



with DAG(
    dag_id='shocktrace_daily_pipeline',
    
    default_args=default_args,
    
    description=(
        'ShockTrace: extract global conflict events, '
        'commodity prices, and disaster data daily'
    ),
    

    schedule_interval='0 7 * * *',
    

    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    
    tags=['shocktrace', 'daily', 'production'],

) as dag:
    

    start = EmptyOperator(
    task_id='start',
    )



    extract_gdelt = PythonOperator(
        task_id='extract_gdelt_conflicts',
        python_callable=extract_gdelt_conflict_events,
    )


    extract_prices = PythonOperator(
        task_id='extract_commodity_prices',
        python_callable=extract_commodity_prices,
    )

    extract_disasters = PythonOperator(
        task_id='extract_disaster_events',
        python_callable=extract_disaster_events,
    )

    extraction_complete = EmptyOperator(
        task_id='extraction_complete',
    )




    end = EmptyOperator(
        task_id='end',
    )




    start >> [extract_gdelt, extract_prices, extract_disasters]
    [extract_gdelt, extract_prices, extract_disasters] >> extraction_complete
    extraction_complete >> end
