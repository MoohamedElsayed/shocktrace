# python script that extracts conflict events in the world from GDLET data 

from google.cloud import bigquery
import os
import logging

logger = logging.getLogger(__name__)


# countries that we track conflict events i nwhen they happen (those are the most to affect the prices "oil, semiconductors, shipping routes") 
HOTSPOT_COUNTRIES = [
    # MiddleEast
    'IRN', 'IRQ', 'YEM', 'ISR', 'PSE', 'SAU', 'SYR', 'LBN',
    # Eastern Europe
    'RUS', 'UKR',
    # East Asia
    'CHN', 'TWN', 'PRK',
    # Africa
    'SDN', 'LBY', 'NGA', 'COD', 'ETH',
    # Latin America
    'VEN', 'COL',
]


# event codes we care about
DISRUPTION_CODES = {
    '14': 'PROTEST',
    '17': 'COERCE',        
    '18': 'ASSAULT',       
    '19': 'FIGHT',          
    '20': 'MASS_VIOLENCE',  
}



def extract_gdelt_conflict_events(**context):
    """
    - Function that extracts gdlet data from big query
    - Airflow will call this function daily to extract yesterday's events
    - context['ds'] will give us the date to extrect its events 'yesterday'
    
    """

    execution_date = context['ds'] 
    formatted_date = execution_date.replace('-', '')

    project_id = os.environ.get('GCP_PROJECT_ID')
    table_ref = f"{project_id}.raw.raw_gdelt_events"

    logger.info(f"Extracting GDELT conflict events for {execution_date}")
    logger.info(f"Monitoring {len(HOTSPOT_COUNTRIES)} countries")


    # filtering via the country and code to get only the data we want
    country_list = ','.join([f"'{c}'" for c in HOTSPOT_COUNTRIES])
    code_list = ','.join([f"'{c}'" for c in DISRUPTION_CODES.keys()])


    # checking if the table exists
    client = bigquery.Client(project=project_id)

    try:
        client.get_table(table_ref)
        table_exists = True
    except Exception:
        table_exists = False

    new_events_sql = f"""
        SELECT
            GLOBALEVENTID AS global_event_id,
            PARSE_DATE('%Y%m%d', CAST(SQLDATE AS STRING)) AS event_date,
            Actor1CountryCode AS actor1_country,
            Actor1Name AS actor1_name,
            Actor2CountryCode AS actor2_country,
            Actor2Name AS actor2_name,
            EventCode AS event_code,
            EventRootCode AS event_root_code,
            CASE EventRootCode
                WHEN '14' THEN 'PROTEST'
                WHEN '17' THEN 'COERCE'
                WHEN '18' THEN 'ASSAULT'
                WHEN '19' THEN 'FIGHT'
                WHEN '20' THEN 'MASS_VIOLENCE'
                ELSE 'OTHER'
            END AS event_type,
            GoldsteinScale AS goldstein_scale,
            NumMentions AS num_mentions,
            NumSources AS num_sources,
            NumArticles AS num_articles,
            AvgTone AS avg_tone,
            ActionGeo_Lat AS action_lat,
            ActionGeo_Long AS action_lon,
            ActionGeo_CountryCode AS action_country,
            ActionGeo_FullName AS action_location_name,
            SOURCEURL AS source_url,
            CURRENT_TIMESTAMP() AS ingested_at,
            '{execution_date}' AS pipeline_date
        FROM `gdelt-bq.gdeltv2.events`
        WHERE SQLDATE = {formatted_date}
            AND EventRootCode IN ({code_list})
            AND (
                Actor1CountryCode IN ({country_list})
                OR Actor2CountryCode IN ({country_list})
                OR ActionGeo_CountryCode IN ({country_list})
            )
    """


    if table_exists:
        logger.info("table exists, appending new data")
        query = f"""
        CREATE OR REPLACE TABLE `{table_ref}` AS
        SELECT * FROM `{table_ref}`
        WHERE pipeline_date != '{execution_date}'
        UNION ALL
        {new_events_sql}
        """
    else:
        logger.info("table doesn't exisr, creating new table")
        query = f"""
        CREATE OR REPLACE TABLE `{table_ref}` AS
        {new_events_sql}
        """

    query_job = client.query(query)
    query_job.result()   

    table = client.get_table(table_ref)
    logger.info(f"GDELT extraction completed")
    logger.info(f"Total rows in table: {table.num_rows}")

    return {"status": "success", "date": execution_date}
    