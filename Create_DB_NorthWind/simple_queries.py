import os

from dotenv import load_dotenv
import pyodbc

import SQL_Queries
from DB_CSV import CSV_reader

load_dotenv()
DRIVER = os.getenv("MS_SQL_DRIVER")
SERVER = os.getenv("MS_SQL_SERVER")
DATABASE = os.getenv('MS_SQL_DATABASE')
PAD_DATABASE = os.getenv('MS_SQL_PAD_DATABASE')
USER = os.getenv('MS_SQL_USER')
PASSWORD = os.getenv('MS_SQL_KEY')

"""SecureConnection"""
connection_string = f'''DRIVER={DRIVER};
                        SERVER={SERVER};
                        DATABASE={DATABASE};
                        UID={USER};
                        PWD={PASSWORD}'''

"""Запускаю запрос на создание базы данных NorthWind"""
create_db = "NorthWind"
conn = pyodbc.connect(connection_string)
conn.autocommit = True

try:
    SQL_Query = SQL_Queries.create_data_base_default(create_db)
    conn.execute(SQL_Query)
except pyodbc.ProgrammingError as ex:
    print(ex)
else:
    print(f"DATABASE {create_db} Created")
finally:
    conn.close()

"""Запускаю запрос на создание таблицы customers_data в БД NorthWind"""
conn = pyodbc.connect(connection_string)
conn.autocommit = True
cursor = conn.cursor()
active_db_name = 'NorthWind'
table_name = 'customers_data'

try:
    SQL_Query = SQL_Queries.create_table_customers_data(table_name)
    conn.execute(fr"USE {active_db_name};")
    conn.execute(SQL_Query)
except pyodbc.ProgrammingError as ex:
    print(ex)
else:
    print(f"TABLE {table_name} Created")
finally:
    conn.close()

"""Запускаю запрос на создание таблицы employees_data в БД NorthWind"""
conn = pyodbc.connect(connection_string)
conn.autocommit = True
cursor = conn.cursor()
active_db_name = 'NorthWind'
table_name = 'employees_data'

try:
    SQL_Query = SQL_Queries.create_table_employees_data(table_name)
    conn.execute(fr"USE {active_db_name};")
    conn.execute(SQL_Query)
except pyodbc.ProgrammingError as ex:
    print(ex)
else:
    print(f"TABLE {table_name} Created")
finally:
    conn.close()

"""Запускаю запрос на создание таблицы orders_data в БД NorthWind"""
conn = pyodbc.connect(connection_string)
conn.autocommit = True
cursor = conn.cursor()
active_db_name = 'NorthWind'
table_name = 'orders_data'

try:
    SQL_Query = SQL_Queries.create_table_orders_data(table_name)
    conn.execute(fr"USE {active_db_name};")
    conn.execute(SQL_Query)
except pyodbc.ProgrammingError as ex:
    print(ex)
else:
    print(f"TABLE {table_name} Created")
finally:
    conn.close()

"""Запускаю запрос на заполнение данными из CSV-файла output_customers_data.csv в таблицу customers_data"""
conn = pyodbc.connect(connection_string)
conn.autocommit = True
cursor = conn.cursor()
active_db_name = 'NorthWind'
table_name = 'customers_data'
filename = 'DB_CSV/files/output_customers_data.csv'

CSV_reader.fill_customers_data_table(cursor, filename, table_name)
conn.close()

"""Запускаю запрос на заполнение данными из CSV-файла output_employees_data.csv в таблицу employees_data"""
conn = pyodbc.connect(connection_string)
conn.autocommit = True
cursor = conn.cursor()
# active_db_name = 'NorthWind'
table_name = 'employees_data'
filename = 'DB_CSV/files/output_employees_data.csv'

CSV_reader.fill_employees_data_table(cursor, filename, table_name)
conn.close()

"""Запускаю запрос на заполнение данными из CSV-файла output_orders_data.csv в таблицу orders_data"""
conn = pyodbc.connect(connection_string)
conn.autocommit = True
cursor = conn.cursor()
# active_db_name = 'NorthWind'
table_name = 'orders_data'
filename = 'DB_CSV/files/output_orders_data.csv'

CSV_reader.fill_orders_data_table(cursor, filename, table_name)
conn.close()
