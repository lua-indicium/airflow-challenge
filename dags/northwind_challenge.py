from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['my_email'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def export_final_answer():
    import base64
    with open('count.txt') as f:
        count = f.readlines()[0]
    my_email = Variable.get("my_email")
    message = my_email + count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt", "w") as f:
        f.write(base64_message)
    return None

def extract_orders():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    query = "SELECT * FROM 'Order'"
    orders = pd.read_sql_query(query, conn)
    orders.to_csv('output_orders.csv', index=False)
    conn.close()

def calculate_quantity_for_rio():
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    query = "SELECT * FROM OrderDetail"
    order_details = pd.read_sql_query(query, conn)
    conn.close()

    orders = pd.read_csv('output_orders.csv')
    merged_data = pd.merge(orders, order_details, left_on='Id', right_on='OrderId')
    quantity_sum = merged_data.loc[
        merged_data['ShipCity'] == 'Rio de Janeiro', 'Quantity'
    ].sum()    

    with open('count.txt', 'w') as f:
        f.write(str(quantity_sum))

with DAG(
    'Northwind',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """Esse é o desafio de Airflow da Indicium."""

    extract_orders_task = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders,
    )

    calculate_quantity_for_rio_task = PythonOperator(
        task_id='calculate_quantity_for_rio',
        python_callable=calculate_quantity_for_rio,
    )

    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True,
    )

    extract_orders_task >> calculate_quantity_for_rio_task >> export_final_output