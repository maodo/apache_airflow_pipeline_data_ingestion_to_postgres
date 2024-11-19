import pandas as pd
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from database import Database

# Use the docker information 
db_config = {
    "host":"postgres-db", # Docker service name
    "port":5432, # docker internal port
    "dbname":"company",
    "user":"company",
    "password":"pass123"
}

db = Database(**db_config)

csv_file_path = '/opt/airflow/employees.csv'
csv_cleaned_file_path = '/opt/airflow/cleaned_employees.csv'
output = './cleaned_employees.csv'



def clean_phone_number(phone_number):
    new_phone = phone_number.split('x',1)[0].replace('.','-').replace('+','00').replace("(","").replace(")","-")
    if'-'not in new_phone:
       new_phone = f'{new_phone[:3]}-{new_phone[3:6]}-{new_phone[6:]}'
    return new_phone
# Let's clean the position and phone_number columns
# Capitalize last_name
def clean_data():
    df = pd.read_csv(csv_file_path)
    df['last_name']= df['last_name'].apply(lambda x:x.upper())
    df['phone_number'] = df['phone_number'].apply(clean_phone_number)
    df['position'] = df['position'].apply(lambda x: x.split(',',1)[0])
    df.to_csv(output, index=False)
    print("Employee data succesfully cleaned !")

def ingest_data():
    db.connect()
    conn = db.conn
    cursor = conn.cursor()
    with open(csv_cleaned_file_path, 'r') as file:
        next(file)
        cursor.copy_from(file, 'employee', sep=',', null='')
    conn.commit()
    print("Data ingested successfully using COPY!")
    cursor.close()
    db.close()

with DAG(
    dag_id='employee_table_ingestion',
    schedule_interval=timedelta(days=1),
    description="Employee table data ingestion DAG",
    start_date=datetime(2024,11,18),
    default_args={
        'owner':'maodo',
        'depends_on_past': False,
        'backfill': False
    }
) as dag:
    start = EmptyOperator(task_id='Start')
    ingest_to_pg =  PythonOperator(
        task_id='ingestion_data',
        python_callable=ingest_data
    )

    clean_data_to_ingest = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )
    end = EmptyOperator(task_id='End')
    
    start >> clean_data_to_ingest >> ingest_to_pg >> end