import requests
import pandas as pd
import time
import io
from os import environ as env
from datetime import datetime,date,timedelta
from google.cloud import bigquery as bq
from google.oauth2 import service_account
from datetime import datetime, timedelta

twod_ago_midnight = datetime.today() - timedelta(days=2) 
twod_ago_midnight = twod_ago_midnight.replace(hour=0, minute=0, second=0, microsecond=0)

# same_d_last_y = datetime.today() - timedelta(days=365) 
# same_d_last_y = same_d_last_y.replace(hour=0, minute=0, second=0, microsecond=0)

key_path = '/Users/cesareborgia/Desktop/9fwr/airflow/google.key.json'
env["GOOGLE_APPLICATION_CREDENTIALS"]=key_path

csvResponse = requests.get('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv').text

table_id='directed-craft-329708.9fwr_af_test.covid_confirmed'

def prepare_data():
    df=pd.read_csv(io.StringIO(csvResponse))
    df.drop(["Province/State",'Lat','Long'],axis=1,inplace=True)
    df=df.rename(columns={"Country/Region": "Country"})

    # un-pivot
    df_up = df.melt(id_vars=['Country'],var_name='Date',value_name='Confirmed')

    df_up.Date = pd.to_datetime(df_up.Date)
    return df_up

def upload_truncate():
    df = prepare_data()
    # LOAD EVERYTHING BUT THE LAST THREE DAYS INTO THE DATAFRAME
    df = df [ df.Date < twod_ago_midnight ]
    df['LoadDate']=pd.to_datetime(datetime.today())

    # UPLOAD TO BIGQUERY
    client = bq.Client()
    job_config = bq.LoadJobConfig(
        # schema=[
        #     bigquery.SchemaField("budget", bigquery.enums.SqlTypeNames.FLOAT)
        # ],
        write_disposition="WRITE_TRUNCATE"
    )
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
        location="europe-west3",  # Must match the destination dataset location.
    )  # Make an API request.
    job.result() 

def upload_append():
    df = prepare_data()
    # ONLY LOAD THE LAST THREE DAYS INTO THE DATAFRAME
    df = df [ df.Date >= twod_ago_midnight ]
    df['LoadDate']=pd.to_datetime(datetime.today())

    # UPLOAD TO BIGQUERY
    client = bq.Client()
    job_config = bq.LoadJobConfig(
        # schema=[
        #     bigquery.SchemaField("budget", bigquery.enums.SqlTypeNames.FLOAT)
        # ],
        write_disposition="WRITE_APPEND"
    )
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
        location="europe-west3",  # Must match the destination dataset location.
    )  # Make an API request.
    job.result() 


