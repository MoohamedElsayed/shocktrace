# script that extracts country-level reference data (population, region, borders)



import requests
import pandas as pd
from google.cloud import bigquery
import os
import logging
from datetime import datetime, timezone




logger = logging.getLogger(__name__)


TRACKED_COUNTRY_CODES = [
    'IRN', 'IRQ', 'YEM', 'ISR', 'PSE', 'SAU', 'SYR', 'LBN',
    'RUS', 'UKR', 'CHN', 'TWN', 'PRK',
    'SDN', 'LBY', 'NGA', 'COD', 'ETH',
    'VEN', 'COL',
    'USA', 'GBR', 'DEU', 'FRA', 'JPN', 'KOR', 'IND',
    'BRA', 'SGP', 'NLD', 'ARE', 'EGY', 'TUR',
]



def extract_reference_data(**context):

    execution_date = context['ds']
    project_id = os.environ.get('GCP_PROJECT_ID')

    logger.info(f"Extracting reference data for {execution_date}")

    all_countries = []


    logger.info("Pulling country metadata from REST Countries")

    try:
        url = "https://restcountries.com/v3.1/all"
        params = {
            'fields': 'cca3,name,population,region,subregion,borders,currencies,latlng',
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        countries_data = response.json()

        country_lookup = {}
        for country in countries_data:
            code = country.get('cca3', '')
            if code in TRACKED_COUNTRY_CODES:
                country_lookup[code] = {
                    'country_code': code,
                    'country_name': country.get('name', {}).get('common', 'Unknown'),
                    'population': country.get('population', 0),
                    'region': country.get('region', 'Unknown'),
                    'subregion': country.get('subregion', 'Unknown'),
                    'borders': ','.join(country.get('borders', [])),
                    'latitude': country.get('latlng', [None, None])[0],
                    'longitude': country.get('latlng', [None, None])[1],
                }

        logger.info(f" Got metadata for {len(country_lookup)} countries")

    except Exception as e:
        logger.warning(f" REST Countries failed: {str(e)}")
        country_lookup = {}


    logger.info("Pulling economic indicators from World Bank")

    wb_indicators = {
        'NY.GDP.MKTP.CD': 'gdp_usd',
        'NE.TRD.GNFS.ZS': 'trade_pct_of_gdp',
        'NE.IMP.GNFS.ZS': 'imports_pct_of_gdp',
    }

    wb_data = {}  

    for indicator_code, indicator_name in wb_indicators.items():
        try:
            url = f"https://api.worldbank.org/v2/country/all/indicator/{indicator_code}"
            params = {
                'format': 'json',
                'per_page': 300,
                'date': '2022:2023',  
                'mrv': 1, 
            }

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if len(data) > 1:
                for entry in data[1]:
                    code = entry.get('countryiso3code', '')
                    value = entry.get('value')
                    if code and value is not None:
                        if code not in wb_data:
                            wb_data[code] = {}
                        wb_data[code][indicator_name] = value

            logger.info(f"Got {indicator_name} for {len(wb_data)} countries")

        except Exception as e:
            logger.warning(f"World Bank {indicator_code} failed: {str(e)}")
            continue


    for code in TRACKED_COUNTRY_CODES:
        country = country_lookup.get(code, {
            'country_code': code,
            'country_name': 'Unknown',
            'population': 0,
            'region': 'Unknown',
            'subregion': 'Unknown',
            'borders': '',
            'latitude': None,
            'longitude': None,
        })

        wb = wb_data.get(code, {})

        combined = {
            **country,
            'gdp_usd': wb.get('gdp_usd'),
            'trade_pct_of_gdp': wb.get('trade_pct_of_gdp'),
            'imports_pct_of_gdp': wb.get('imports_pct_of_gdp'),
            'ingested_at': datetime.utcnow().isoformat(),
            'pipeline_date': execution_date,
        }

        all_countries.append(combined)

    if all_countries:
        df = pd.DataFrame(all_countries)

        df['population'] = pd.to_numeric(df['population'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['gdp_usd'] = pd.to_numeric(df['gdp_usd'], errors='coerce')
        df['trade_pct_of_gdp'] = pd.to_numeric(df['trade_pct_of_gdp'], errors='coerce')
        df['imports_pct_of_gdp'] = pd.to_numeric(df['imports_pct_of_gdp'], errors='coerce')
        df['pipeline_date'] = pd.to_datetime(df['pipeline_date']).dt.date
        df['ingested_at'] = pd.to_datetime(df['ingested_at'])
        df = df.where(df.notnull(), None)

        client = bigquery.Client(project=project_id)
        table_id = f'{project_id}.raw.raw_country_reference'


        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("country_code", "STRING"),
                bigquery.SchemaField("country_name", "STRING"),
                bigquery.SchemaField("population", "INT64"),
                bigquery.SchemaField("region", "STRING"),
                bigquery.SchemaField("subregion", "STRING"),
                bigquery.SchemaField("borders", "STRING"),
                bigquery.SchemaField("latitude", "FLOAT64"),
                bigquery.SchemaField("longitude", "FLOAT64"),
                bigquery.SchemaField("gdp_usd", "FLOAT64"),
                bigquery.SchemaField("trade_pct_of_gdp", "FLOAT64"),
                bigquery.SchemaField("imports_pct_of_gdp", "FLOAT64"),
                bigquery.SchemaField("ingested_at", "TIMESTAMP"),
                bigquery.SchemaField("pipeline_date", "DATE"),
            ],
        )

        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()

        logger.info(f"Loaded {len(df)} country records into BigQuery")
        return {"status": "success", "records": len(df)}

    else:
        logger.warning("No reference data retrieved")
        return {"status": "no_data", "records": 0}