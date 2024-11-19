import shortuuid
from faker import Faker
import random
from employee import Employee
import pandas as pd
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from datetime import datetime


fake = Faker()
num_rows = 200
employees = []
start_date = datetime(2024,11,18)
output_file = './employees.csv'
jobs = ['Data Engineer', 'Data Science Engineer', 'Machine Learning Engineer','Data Analyst']
def generate_random_data()-> Employee:
    id = shortuuid.uuid()
    first_name = fake.first_name()
    last_name = fake.last_name()
    phone_number = fake.phone_number()
    address = fake.street_address()
    email_address = fake.company_email()
    position = fake.job()
    salary = random.randint(1000, 100000)
    employee = Employee(id,first_name, last_name, phone_number, address,email_address, position, salary)
    return  employee

def generate_employee_data():
    intital_row_idx = 0
    while intital_row_idx < num_rows:
        employee = generate_random_data()
        employees.append(employee)
        intital_row_idx += 1
    data= [{"employee_id": employee.id, "first_name": employee.first_name, 
            "last_name": employee.last_name, "phone_number":employee.phone_number,
            "address":employee.address, "email_address":employee.email_address,
            "position":employee.position,"salary":employee.salary
            } for employee in employees]
    df = pd.DataFrame(data)
    df.to_csv(output_file,index=False)
    print(f'Employee data succesfully generated !')

with DAG(
    dag_id="employees_data_generation",
    default_args={
        "depends_on_past": False,
        "owner": "maodo",
        "backfill": False
    },
    schedule_interval='@daily',
    start_date=start_date,
    description='Employees data generation DAG'
) as dag:
    start = EmptyOperator(task_id='start')

    generate_data = PythonOperator(
        task_id='generate_employee_data',
        python_callable=generate_employee_data
        )
    ingest_data = TriggerDagRunOperator(
        task_id='trigger_ingest_data',
        trigger_dag_id = 'employee_table_ingestion'
    )
    
    end = EmptyOperator(task_id='end')

    start >> generate_data >> end >> ingest_data