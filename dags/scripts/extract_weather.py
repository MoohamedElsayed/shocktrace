# script that extracts daily weather data



import requests
import pandas as pd
from google.cloud import bigquery
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)



MONITORED_REGIONS = {
    'us_midwest': {
        'name': 'US Midwest (Corn Belt)',
        'lat': 41.5,
        'lon': -93.0,
        'commodities': 'corn, soybeans',
    },
    'us_gulf': {
        'name': 'US Gulf Coast (Oil Refineries)',
        'lat': 29.7,
        'lon': -95.3,
        'commodities': 'oil refining',
    },
    'ukraine': {
        'name': 'Ukraine (Wheat Belt)',
        'lat': 49.0,
        'lon': 32.0,
        'commodities': 'wheat, sunflower oil',
    },
    'brazil_south': {
        'name': 'Brazil South (Coffee)',
        'lat': -22.9,
        'lon': -47.0,
        'commodities': 'coffee, sugar',
    },
    'india_north': {
        'name': 'India North (Rice, Wheat)',
        'lat': 28.6,
        'lon': 77.2,
        'commodities': 'rice, wheat, spices',
    },
    'southeast_asia': {
        'name': 'Southeast Asia (Palm Oil, Rice)',
        'lat': 3.1,
        'lon': 101.7,
        'commodities': 'palm oil, rice, rubber',
    },
    'middle_east': {
        'name': 'Middle East (Oil Region)',
        'lat': 26.0,
        'lon': 50.5,
        'commodities': 'oil, natural gas',
    },
    'europe_west': {
        'name': 'Western Europe (Industry)',
        'lat': 50.1,
        'lon': 8.7,
        'commodities': 'manufacturing, energy demand',
    },
}



def extract_weather_data(**context):


    execution_date = context['ds']
    project_id = os.environ.get('GCP_PROJECT_ID')

    logger.info(f"Extracting weather data for {execution_date}")
    logger.info(f"Monitoring {len(MONITORED_REGIONS)} agricultural regions")

    all_weather = []



    for region_id, region in MONITORED_REGIONS.items():

        try:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                'latitude': region['lat'],
                'longitude': region['lon'],
                'start_date': execution_date,
                'end_date': execution_date,
                'daily': ','.join([
                    'temperature_2m_max',
                    'temperature_2m_min',
                    'temperature_2m_mean',
                    'precipitation_sum',
                    'windspeed_10m_max',
                ]),
                'timezone': 'UTC',
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            daily = data.get('daily', {})

            if daily and daily.get('time'):
                weather_record = {
                    'region_id': region_id,
                    'region_name': region['name'],
                    'commodities_affected': region['commodities'],
                    'latitude': region['lat'],
                    'longitude': region['lon'],
                    'observation_date': daily['time'][0],
                    'temp_max_c': daily.get('temperature_2m_max', [None])[0],
                    'temp_min_c': daily.get('temperature_2m_min', [None])[0],
                    'temp_mean_c': daily.get('temperature_2m_mean', [None])[0],
                    'precipitation_mm': daily.get('precipitation_sum', [None])[0],
                    'wind_max_kmh': daily.get('windspeed_10m_max', [None])[0],
                    'ingested_at': datetime.now(timezone.utc).isoformat(),
                    'pipeline_date': execution_date,
                }
                all_weather.append(weather_record)

                logger.info(
                    f"{region['name']}: "
                    f"{weather_record['temp_mean_c']}°C, "
                    f"{weather_record['precipitation_mm']}mm rain"
                )

        except Exception as e:
            logger.warning(f"Failed for {region_id}: {str(e)}")
            continue



    if all_weather:
        df = pd.DataFrame(all_weather)

        df['observation_date'] = pd.to_datetime(df['observation_date']).dt.date
        df['pipeline_date'] = pd.to_datetime(df['pipeline_date']).dt.date
        df['ingested_at'] = pd.to_datetime(df['ingested_at'])
        for col in ['temp_max_c', 'temp_min_c', 'temp_mean_c', 'precipitation_mm', 'wind_max_kmh']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.where(df.notnull(), None)

        client = bigquery.Client(project=project_id)
        table_id = f'{project_id}.raw.raw_weather_data'

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("region_id", "STRING"),
                bigquery.SchemaField("region_name", "STRING"),
                bigquery.SchemaField("commodities_affected", "STRING"),
                bigquery.SchemaField("latitude", "FLOAT64"),
                bigquery.SchemaField("longitude", "FLOAT64"),
                bigquery.SchemaField("observation_date", "DATE"),
                bigquery.SchemaField("temp_max_c", "FLOAT64"),
                bigquery.SchemaField("temp_min_c", "FLOAT64"),
                bigquery.SchemaField("temp_mean_c", "FLOAT64"),
                bigquery.SchemaField("precipitation_mm", "FLOAT64"),
                bigquery.SchemaField("wind_max_kmh", "FLOAT64"),
                bigquery.SchemaField("ingested_at", "TIMESTAMP"),
                bigquery.SchemaField("pipeline_date", "DATE"),
            ],
        )

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()

        logger.info(f"Loaded {len(df)} weather records into BigQuery")
        return {"status": "success", "records": len(df)}

    else:
        logger.warning("No weather data retrieved")
        return {"status": "no_data", "records": 0}