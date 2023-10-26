import datetime as dt
from datetime import datetime, timedelta
from airflow import DAG
from elasticsearch import Elasticsearch
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db

# Function to get data from PostgreSQL
def get_data_from_db():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m2", conn)  
    df.to_csv('/opt/airflow/dags/data.csv',index=False)
    
def data_pipeline():
    #Loading CSV to dataframe
    df_data = pd.read_csv('/opt/airflow/dags/data.csv')

    #### start transformation
    # Remove null values.
    df_data.dropna(inplace=True)
    df_data.to_csv('/opt/airflow/dags/data_clean.csv', index=False)


# Function to post the data to Kibana
def post_to_kibana():
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="table_m2", id=i+1, body=doc)
        # print(res)


# DAG setup
default_args = {
    'owner': 'irfan',
    'depends_on_past': False,
    'email_on_failure': False, #Parameter ini mengontrol apakah notifikasi email akan dikirim jika task mengalami kegagalan.
    'email_on_retry': False, #Parameter ini mengontrol apakah notifikasi email akan dikirim jika task dijadwalkan ulang (retry).
    'retries': 1, #menentukan berapa kali task akan mencoba dijalankan ulang jika terjadi kegagalan.
    'retry_delay': timedelta(minutes=60), #menentukan berapa lama (dalam satuan waktu) Apache Airflow harus menunggu sebelum mencoba menjalankan ulang task jika terjadi kegagalan. Dalam kasus ini, task akan dijadwalkan ulang setiap 60 menit (1 jam) jika diperlukan
    #
}

with DAG('irfan_LC8',
         description='tugas LC',
         default_args=default_args,
         schedule_interval='@daily', # mengatur frekuensi eksekusi DAG. Dalam hal ini, DAG ini dijadwalkan untuk berjalan setiap hari
         start_date=datetime(2023, 10, 1), #menunjukkan tanggal dan waktu saat DAG akan mulai dijalankan. 1 Oktober 2023
         catchup=False) as dag: #Airflow tidak akan mengejar eksekusi yang tertinggal sebelum tanggal start_date. 
        #Jika ada pekerjaan yang seharusnya dijalankan di hari-hari sebelum tanggal mulai, itu tidak akan dieksekusi secara otomatis
    
    # Task to fetch data from PostgreSQL
    fetch_task = PythonOperator(
        task_id='get_data_from_db',
        python_callable=get_data_from_db
    )
    
    # Task yg akan di eksekusi pythonoperator
    clean_task = PythonOperator(
        task_id='cleaning_data',
        python_callable=data_pipeline
    )
    
     # Task to post to Kibana
    post_to_kibana_task = PythonOperator(
        task_id='post_to_kibana',
        python_callable=post_to_kibana
    )
    # Set task dependencies
    #baris yang mencoba menentukan hubungan ketergantungan antara task `clean_task`
    #namun disini kita hanya menyertakan clean_task tanpa menentukan hubungan ketergantungan.
    clean_task
    #fetch_task >> clean_task >> post_to_kibana_task