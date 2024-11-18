import shortuuid
from faker import Faker
import random
from employee import Employee
import pandas as pd


fake = Faker()
num_rows = 10
employees = []
output_file = './employees.csv'
def generate_random_data()-> Employee:
    id = shortuuid.uuid()
    first_name = fake.first_name()
    last_name = fake.last_name()
    phone_number = fake.phone_number()
    address = fake.address()
    email_address = fake.company_email()
    position = fake.job()
    salary = random.randint(1000, 100000)
    employee = Employee(id,first_name, last_name, phone_number, address,email_address, position, salary)
    return  employee

def generate_employee_data():
    intital_row_idx = 1
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

generate_employee_data()
