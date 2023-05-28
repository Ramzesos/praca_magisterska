from datetime import timedelta
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import random
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
import sqlite3
import datetime
import time
import requests
import random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException

from otodom_etl import run_otodom_etl

default_args = {
    'owner': 'bartek',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 3),
    'email': ['bartoszradz@gmail.com.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'otodom_dag',
    default_args=default_args,
    description='My otodom DAG with ETL process',
    schedule_interval=timedelta(days=7),
)

run_etl = PythonOperator(
    task_id='whole_otodom_etl',
    python_callable=run_otodom_etl,
    dag=dag,
)

run_etl


