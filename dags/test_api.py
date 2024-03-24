from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from googleapiclient.discovery import build


# Spreadsheet
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import gspread

with DAG(
    dag_id="dataframe_to_spreadsheet",
    start_date=datetime.now(),
    schedule_interval="@daily",
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    @task()
    def dataframe_to_spreadsheet_task():

        # Hook to Google Sheets in order to get connection from Airflow
        hook = GoogleBaseHook(gcp_conn_id="google_conn_id")
        credentials = hook.get_credentials()
        google_credentials = gspread.Client(auth=credentials)

        # Reading a spreadsheet by its title
        sheet = google_credentials.open("Products - Data")

        # Defining the worksheet to manipulate
        worksheet = sheet.worksheet("products-data")

        # get all the values
        import pandas as pd
        import os

        hasil = worksheet.get_all_records()

        # Change to list
        list = pd.DataFrame.from_records(hasil)

        # send it to the csv file
        current = os.getcwd()
        dir = current + '/dags/contoh.csv'

        df = list.to_csv(dir,  index=False)

        return df

    @task()
    def to_csv():
        import csv

    start >> dataframe_to_spreadsheet_task() >> end
