# script to extract detailed energy data from the US Energy information administration (EIA) API 





import requests
import pandas as pd
from google.cloud import bigquery
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)



EIA_SERIES = {
    'PET.WCESTUS1.W': {
        'name': 'us_crude_oil_stocks_thousand_barrels',
        'frequency': 'weekly',
    },
    'PET.WCRFPUS2.W': {
        'name': 'us_crude_oil_production_thousand_barrels_per_day',
        'frequency': 'weekly',
    },
    'PET.WPULEUS3.W': {
        'name': 'us_refinery_utilization_pct',
        'frequency': 'weekly',
    },
    'PET.WCEIMUS2.W': {
        'name': 'us_crude_oil_imports_thousand_barrels_per_day',
        'frequency': 'weekly',
    },
}



def extract_energy_data(**context):

    execution_date = context['ds']
    eia_key = os.environ.get('EIA_API_KEY')
    project_id = os.environ.get('GCP_PROJECT_ID')

    logger.info(f"Extracting EIA energy data for {execution_date}")

    all_energy = []

    for series_id, series_info in EIA_SERIES.items():
        try:


            url = "https://api.eia.gov/v2/seriesid/" + series_id
            params = {
                'api_key': eia_key,
                'frequency': series_info['frequency'],
                'data[0]': 'value',
                'start': execution_date.replace('-', ''),
                'sort[0][column]': 'period',
                'sort[0][direction]': 'desc',
                'length': 1,
            }

            response = requests.get(url, params=params, timeout=30)

            if response.status_code != 200:
                url_v1 = "https://api.eia.gov/series/"
                params_v1 = {
                    'api_key': eia_key,
                    'series_id': series_id,
                    'num': 1,
                }
                response = requests.get(url_v1, params=params_v1, timeout=30)
                response.raise_for_status()
                data = response.json()

                series_data = data.get('series', [{}])[0]
                observations = series_data.get('data', [])

                for obs in observations:
                    if obs[1] is not None:
                        all_energy.append({
                            'series_id': series_id,
                            'series_name': series_info['name'],
                            'frequency': series_info['frequency'],
                            'period': str(obs[0]),
                            'value': float(obs[1]),
                            'ingested_at': datetime.utcnow().isoformat(),
                            'pipeline_date': execution_date,
                        })
                        logger.info(
                            f"{series_info['name']}: {obs[1]}"
                        )
            else:
                data = response.json()
                for item in data.get('response', {}).get('data', []):
                    if item.get('value') is not None:
                        all_energy.append({
                            'series_id': series_id,
                            'series_name': series_info['name'],
                            'frequency': series_info['frequency'],
                            'period': item.get('period', ''),
                            'value': float(item['value']),
                            'ingested_at': datetime.now(timezone.utc).isoformat(),
                            'pipeline_date': execution_date,
                        })
                        logger.info(
                            f"{series_info['name']}: {item['value']}"
                        )

        except Exception as e:
            logger.warning(
                f"Failed to get {series_id}: {str(e)}"
            )
            continue

    if all_energy:
        df = pd.DataFrame(all_energy)

        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df['ingested_at'] = pd.to_datetime(df['ingested_at'])
        df['pipeline_date'] = pd.to_datetime(df['pipeline_date']).dt.date

        client = bigquery.Client(project=project_id)
        table_id = f'{project_id}.raw.raw_energy_data'

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("series_id", "STRING"),
                bigquery.SchemaField("series_name", "STRING"),
                bigquery.SchemaField("frequency", "STRING"),
                bigquery.SchemaField("period", "STRING"),
                bigquery.SchemaField("value", "FLOAT64"),
                bigquery.SchemaField("ingested_at", "TIMESTAMP"),
                bigquery.SchemaField("pipeline_date", "DATE"),
            ],
        )

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()

        logger.info(f"Loaded {len(df)} energy records into BigQuery")
        return {"status": "success", "records": len(df)}

    else:
        logger.warning("No energy data available (may update weekly)")
        return {"status": "no_data", "records": 0}

