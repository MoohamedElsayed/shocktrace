# main DAG that runs daily


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
import pendulum

from scripts.extract_gdelt import extract_gdelt_conflict_events
from scripts.extract_prices import extract_commodity_prices
from scripts.extract_disasters import extract_disaster_events
from scripts.extract_weather import extract_weather_data
from scripts.extract_energy import extract_energy_data
from scripts.extract_reference import extract_reference_data


daily_args = {
    'owner': 'shocktrace',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}




with DAG(
    dag_id='shocktrace_daily_pipeline',
    default_args=daily_args,
    description='Daily: conflicts + prices + disasters + weather + energy',
    schedule_interval='0 7 * * *',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    tags=['shocktrace', 'daily', 'production'],
) as daily_dag:
    

    start = EmptyOperator(task_id='start')



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

    extract_weather = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data,
    )

    extract_energy = PythonOperator(
        task_id='extract_energy_data',
        python_callable=extract_energy_data,
    )

    extraction_complete = EmptyOperator(
        task_id='extraction_complete',
    )




    end = EmptyOperator(task_id='end')





   
    start >> [
        extract_gdelt,
        extract_prices,
        extract_disasters,
        extract_weather,
        extract_energy,
    ]

    [
        extract_gdelt,
        extract_prices,
        extract_disasters,
        extract_weather,
        extract_energy,
    ] >> extraction_complete >> end







weekly_args = {
    'owner': 'shocktrace',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='shocktrace_weekly_reference',
    default_args=weekly_args,
    description='Weekly: country metadata + economic indicators',
    schedule_interval='0 6 * * 0', 
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    tags=['shocktrace', 'weekly', 'reference'],
) as weekly_dag:

    start_ref = EmptyOperator(task_id='start')

    extract_ref = PythonOperator(
        task_id='extract_reference_data',
        python_callable=extract_reference_data,
    )

    end_ref = EmptyOperator(task_id='end')

    start_ref >> extract_ref >> end_ref