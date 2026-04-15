# extracts active natural disaster alerts from GDACS (Global Disaster Alert Coordination System) and USGS (US Geological Survey - earthquakes)


import requests
import xml.etree.ElementTree as ET
import pandas as pd
from google.cloud import bigquery
import os
import logging
from datetime import datetime



logger = logging.getLogger(__name__)


def extract_disaster_events(**context):
    """
    extracts active disaster alerts and recent earthquakes
    """
    execution_date = context['ds']
    project_id = os.environ.get('GCP_PROJECT_ID')

    all_disasters = []

    logger.info(f"Extracting GDACS disaster alerts for {execution_date}")

    try:
        gdacs_url = "https://www.gdacs.org/xml/rss.xml"
        response = requests.get(gdacs_url, timeout=30)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        
        for item in root.findall('.//item'):
            disaster = {
                'event_id': _get_text(
                    item, '{http://www.gdacs.org}eventid'
                ) or 'UNKNOWN',
                'event_type': _get_text(
                    item, '{http://www.gdacs.org}eventtype'
                ) or 'UNKNOWN',
                'alert_level': _get_text(
                    item, '{http://www.gdacs.org}alertlevel'
                ) or 'GREEN',
                'severity_value': _get_text(
                    item, '{http://www.gdacs.org}severity'
                ),
                'event_name': _get_text(item, 'title') or 'Unknown Event',
                'description': _get_text(
                    item, 'description'
                ),
                'pub_date': _get_text(item, 'pubDate'),
                'latitude': _safe_float(_get_text(
                    item, 
                    '{http://www.w3.org/2003/01/geo/wgs84_pos#}lat'
                )),
                'longitude': _safe_float(_get_text(
                    item, 
                    '{http://www.w3.org/2003/01/geo/wgs84_pos#}long'
                )),
                'country': _get_text(
                    item, '{http://www.gdacs.org}country'
                ) or 'UNKNOWN',
                'source': 'GDACS',
                'ingested_at': datetime.utcnow().isoformat(),
                'pipeline_date': execution_date,
            }
            all_disasters.append(disaster)
        
        logger.info(f"Found {len(all_disasters)} GDACS alerts")
        
    except Exception as e:
        logger.warning(f"GDACS extraction failed: {str(e)}")


    logger.info(f"Extracting USGS earthquake data for {execution_date}")


    try:
        usgs_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        params = {
            'format': 'geojson',
            'starttime': execution_date,
            'endtime': execution_date,
            'minmagnitude': 5,
        }
        
        response = requests.get(usgs_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        quake_count = 0
        for feature in data.get('features', []):
            props = feature.get('properties', {})
            coords = feature.get('geometry', {}).get('coordinates', [])
            
            earthquake = {
                'event_id': f"USGS_{props.get('code', 'UNKNOWN')}",
                'event_type': 'EARTHQUAKE',
                'alert_level': (props.get('alert') or 'green').upper(),
                'severity_value': str(props.get('mag', 0)),
                'event_name': props.get('title', 'Unknown Earthquake'),
                'description': (
                    f"Magnitude {props.get('mag', '?')} earthquake"
                ),
                'pub_date': datetime.fromtimestamp(
                    props.get('time', 0) / 1000
                ).isoformat() if props.get('time') else None,
                'latitude': coords[1] if len(coords) > 1 else None,
                'longitude': coords[0] if len(coords) > 0 else None,
                'country': props.get('place', 'UNKNOWN'),
                'source': 'USGS',
                'ingested_at': datetime.utcnow().isoformat(),
                'pipeline_date': execution_date,
            }
            all_disasters.append(earthquake)
            quake_count += 1
        
        logger.info(f"Found {quake_count} significant earthquakes")
        
    except Exception as e:
        logger.warning(f"USGS extraction failed: {str(e)}")


    
    if all_disasters:
        df = pd.DataFrame(all_disasters)

        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['pipeline_date'] = pd.to_datetime(df['pipeline_date']).dt.date
        df['ingested_at'] = pd.to_datetime(df['ingested_at'])
        
        df = df.where(df.notnull(), None)
        
        client = bigquery.Client(project=project_id)
        table_id = f'{project_id}.raw.raw_disaster_events'
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("event_id", "STRING"),
                bigquery.SchemaField("event_type", "STRING"),
                bigquery.SchemaField("alert_level", "STRING"),
                bigquery.SchemaField("severity_value", "STRING"),
                bigquery.SchemaField("event_name", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("pub_date", "STRING"),
                bigquery.SchemaField("latitude", "FLOAT64"),
                bigquery.SchemaField("longitude", "FLOAT64"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("source", "STRING"),
                bigquery.SchemaField("ingested_at", "TIMESTAMP"),
                bigquery.SchemaField("pipeline_date", "DATE"),
            ],
        )
        
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        
        logger.info(f"Loaded {len(df)} disaster events into BigQuery")
        return {"status": "success", "records": len(df)}
    
    else:
        logger.warning("No disaster events found")
        return {"status": "no_data", "records": 0}
    

def _get_text(element, tag):
    """function to get text from an XML element"""
    found = element.find(tag)
    return found.text.strip() if found is not None and found.text else None    

def _safe_float(value):
    """function to convert to float, return None if impossible."""
    try:
        return float(value) if value else None
    except (ValueError, TypeError):
        return None


        