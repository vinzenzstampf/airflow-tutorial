import requests
import pandas as pd
import time
import io
from os import environ as env
from datetime import datetime,date,timedelta
from google.cloud import bigquery as bq
from google.oauth2 import service_account


def upload():
    key_path = '/Users/cesareborgia/Desktop/9fwr/airflow/google.key.json'

    env["GOOGLE_APPLICATION_CREDENTIALS"]=key_path

    csvResponse = requests.get('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv').text

    df=pd.read_csv(io.StringIO(csvResponse))
    df.drop(["Province/State",'Lat','Long'],axis=1,inplace=True)
    df=df.rename(columns={"Country/Region": "Country"})
    df_up = df.melt(id_vars=['Country'],var_name='Date',value_name='Confirmed')

    table_id='directed-craft-329708.9fwr_af_test.covid_confirmed'

    client = bq.Client()
    job_config = bq.LoadJobConfig(
        # schema=[
        #     bigquery.SchemaField("budget", bigquery.enums.SqlTypeNames.FLOAT)
        # ],
        write_disposition="WRITE_TRUNCATE"
    )
    job = client.load_table_from_dataframe(
        df_up,
        table_id,
        job_config=job_config,
        location="europe-west3",  # Must match the destination dataset location.
    )  # Make an API request.
    job.result() 
upload()
