from datetime import datetime
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import requests
import logging

from io import StringIO

dag = DAG(
    'cotacoes_bcb_classic',
    schedule_interval='@daily',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'start_date': datetime(2023, 1, 1),
        'catchup': False,
    },
    tags=['bcb']
)

def extract(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    base_url = 'https://www4.bcb.gov.br/Download/Fechamento/'
    full_url = base_url + ds_nodash + ".csv"
    logging.warning(full_url)

    try:
        response = requests.get(full_url)
        if response.status_code == 200:
            csv_data = response.content.decode('utf-8')
            return csv_data
    except Exception as e:
        logging.error(e)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag
)

def transform(**kwargs):
    cotacoes = kwargs['ti'].xcom_pull(task_ids='extract')
    csvStringIO = StringIO(cotacoes)

    column_names = [
        "DT_FECHAMENTO",
        "COD_MOEDA",
        "TIPO_MOEDA",
        "DESC_MOEDA",
        "TAXA_COMPRA",
        "TAXA_VENDA",
        "PARIDADE_COMPRA",
        "PARIDADE_VENDA"
    ]

    data_types = {
        "DT_FECHAMENTO": str,
        "COD_MOEDA": str,
        "TIPO_MOEDA": str,
        "DESC_MOEDA": str,
        "TAXA_COMPRA": float,
        "TAXA_VENDA": float,
        "PARIDADE_COMPRA": float,
        "PARIDADE_VENDA": float
    }

    parse_dates = ['DT_FECHAMENTO']

    df = pd.read_csv(
        csvStringIO,
        sep=";",
        decimal=",",
        thousands=".",
        encoding="utf-8",
        header=None,
        names=column_names,
        dtype=data_types,
        parse_dates=parse_dates
    )

    df['data_processamento'] = datetime.now()
    
    return df

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag
)