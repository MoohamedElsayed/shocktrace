# script to pull daily commodity prices like (oil, gas, wheat) from FRED API and currencies exchange rates from Frankfurter API

import requests
import pandas as pd
from google.cloud import bigquery
import os
import logging
from datetime import datetime, timedelta, timezone


logger = logging.getLogger(__name__)


FRED_SERIES = {
    'DCOILBRENTEU': 'brent_crude_usd_per_barrel',
    'DCOILWTICO': 'wti_crude_usd_per_barrel',
    
    'DHHNGSP': 'natural_gas_usd_per_mmbtu',
    
    'GASREGW': 'us_regular_gas_price_per_gallon',
}


CURRENCIES = [
    'EUR', 'GBP', 'JPY', 'CNY', 'RUB',
    'SAR', 'TRY', 'EGP', 'INR', 'BRL',
]

def extract_commodity_prices(**context):
    """
    extracts yesterday's commodity prices and currency exchange rates and loads them into BigQuery
    """

    execution_date = context['ds']
    fred_key = os.environ.get('FRED_API_KEY')
    project_id = os.environ.get('GCP_PROJECT_ID')
    

    logger.info(f"extracting commodity prices for {execution_date}")

    all_prices = []

    for series_id, friendly_name in FRED_SERIES.items():
        try:
            url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': fred_key,
                'file_type': 'json',
                'observation_start': execution_date,
                'observation_end': execution_date,
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            for obs in data.get('observations', []):
                if obs['value'] != '.':
                    all_prices.append({
                        'price_date': obs['date'],
                        'series_id': series_id,
                        'series_name': friendly_name,
                        'price_value': float(obs['value']),
                        'source': 'FRED',
                        'ingested_at': datetime.now(timezone.utc).isoformat(),
                        'pipeline_date': execution_date,
                    })
                    logger.info(
                        f"{friendly_name}: ${obs['value']}"
                    )

        except Exception as e:
            logger.warning(
                f"Failed to get {series_id}: {str(e)}"
            )
            continue


    try:
        logger.info(f"Extracting exchange rates for {execution_date}")
        
        fx_url = f"https://api.frankfurter.app/{execution_date}"
        params = {
            'from': 'USD',
            'to': ','.join(CURRENCIES),
        }
        
        response = requests.get(fx_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        for currency, rate in data.get('rates', {}).items():
            all_prices.append({
                'price_date': execution_date,
                'series_id': f'FX_USD_{currency}',
                'series_name': f'usd_to_{currency.lower()}',
                'price_value': float(rate),
                'source': 'FRANKFURTER',
                'ingested_at': datetime.now(timezone.utc).isoformat(),
                'pipeline_date': execution_date,
            })
            logger.info(f"USD/{currency}: {rate}")


    except Exception as e:
        logger.warning(f"Failed to get exchange rates: {str(e)}")



    if all_prices:
        df = pd.DataFrame(all_prices)


        df['price_date'] = pd.to_datetime(df['price_date']).dt.date
        df['price_value'] = pd.to_numeric(df['price_value'], errors='coerce')
        df['ingested_at'] = pd.to_datetime(df['ingested_at'])
        df['pipeline_date'] = pd.to_datetime(df['pipeline_date']).dt.date
        
        client = bigquery.Client(project=project_id)
        table_id = f'{project_id}.raw.raw_commodity_prices'
        
        job_config = bigquery.LoadJobConfig(

            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("price_date", "DATE"),
                bigquery.SchemaField("series_id", "STRING"),
                bigquery.SchemaField("series_name", "STRING"),
                bigquery.SchemaField("price_value", "FLOAT64"),
                bigquery.SchemaField("source", "STRING"),
                bigquery.SchemaField("ingested_at", "TIMESTAMP"),
                bigquery.SchemaField("pipeline_date", "DATE"),
            ],
        )
        
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        
        logger.info(f"Loaded {len(df)} price records into BigQuery")
        return {"status": "success", "records": len(df)}
    
    else:
        logger.warning("No price data available for this date")
        logger.warning("Markets might be closed — weekends/holidays")
        return {"status": "no_data", "records": 0}