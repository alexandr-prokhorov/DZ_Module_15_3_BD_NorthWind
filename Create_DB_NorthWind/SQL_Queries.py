"""Функция по созданию Базы Данных"""


def create_data_base_default(name):
    COMMAND = fr"""CREATE DATABASE {name};"""
    return COMMAND


"""Функция по созданию таблицы customers_data в БД NorthWind"""


def create_table_customers_data(name):
    COMMAND = fr"""CREATE TABLE {name}
                (customer_id nvarchar(10) PRIMARY KEY,
                 company_name nvarchar(100),
                 contract_name nvarchar(50));"""
    return COMMAND


"""Функция по созданию таблицы employees_data в БД NorthWind"""


def create_table_employees_data(name):
    COMMAND = fr"""CREATE TABLE {name}
                (employee_id int PRIMARY KEY,
                 first_name nvarchar(100),
                 last_name nvarchar(100),
                 title nvarchar(100),
                 birth_data date,
                 notes nvarchar(1000));"""
    return COMMAND


"""Функция по созданию таблицы orders_data в БД NorthWind"""


def create_table_orders_data(name):
    COMMAND = fr"""CREATE TABLE {name}
                (order_id int PRIMARY KEY,
                 customer_id nvarchar(10),
                 employee_id int,
                 order_date date,
                 ship_city nvarchar(100),
                 CONSTRAINT fk_customer
                        FOREIGN KEY (customer_id)
                        REFERENCES customers_data(customer_id),
                 CONSTRAINT fr_employee
                        FOREIGN KEY (employee_id)
                        REFERENCES employees_data(employee_id));"""
    return COMMAND
