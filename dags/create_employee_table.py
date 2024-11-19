from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
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

def create_emp_table():
    create_table_query = """
        CREATE TABLE employee (
            employee_id VARCHAR(255) NOT NULL,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            phone_number VARCHAR(40),
            address TEXT,
            email_address VARCHAR(100) UNIQUE,
            position VARCHAR(50),
            salary NUMERIC(10, 2) CHECK (salary >= 0)
        );"""
    db.connect()
    conn = db.conn
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    print(f'Employee table succesfully created !')
    db.close()

with DAG(
    dag_id='employee_table_creation',
    schedule_interval=timedelta(days=1),
    description="Employee table creation DAG",
    start_date=datetime(2024,11,18),
    default_args={
        'owner':'maodo',
        'depends_on_past': False,
        'backfill': False
    }
) as dag:
    start = EmptyOperator(task_id='start')
    create_employee_table = PythonOperator(
        task_id='create_employee_table',
        python_callable=create_emp_table
        )
    end = EmptyOperator(task_id='end')

    start >> create_employee_table >> end