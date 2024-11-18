
class Employee(object):
    def __init__(self,employee_id,first_name, last_name, phone_number, address,email_address, position, salary):
        super(object, self).__init__()
        self.id = employee_id
        self.first_name = first_name
        self.last_name = last_name
        self.phone_number = phone_number
        self.address = address
        self.email_address = email_address
        self.position = position
        self.salary = salary

    