from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from googleapiclient.discovery import build


# Spreadsheet
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import gspread

import pandas as pd

with DAG(
    dag_id="dataframe_to_spreadsheet",
    start_date=datetime.now(),
    schedule_interval="@daily",
) as dag:

    @task()
    def dataframe_to_spreadsheet_task():
        data = {
            "products": ["aneh", "ngehe", "keren"],
            "price": [50, 40, 100],
        }

        df = pd.DataFrame(data)

        # Hook to Google Sheets in order to get connection from Airflow
        hook = GoogleBaseHook(gcp_conn_id="google_conn_id")
        credentials = hook.get_credentials()
        google_credentials = gspread.Client(auth=credentials)

        # Reading a spreadsheet by its title
        sheet = google_credentials.open("Products - Data")

        # Defining the worksheet to manipulate
        worksheet = sheet.worksheet("products-data")

        # get all the values        
        return worksheet.get_values()

    dataframe_to_spreadsheet_task()
