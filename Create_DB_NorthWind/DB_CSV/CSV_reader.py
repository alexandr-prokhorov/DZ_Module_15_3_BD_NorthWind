import csv
import pyodbc

"""Создание функции по чтению файла формата CSV"""


def get_csv_data(filename):
    data_from_csv = []
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            data_from_csv.append(row)
    return data_from_csv


"""Создание функции по записи данных в формат CSV"""


def write_csv_in_file(filename, data):
    with open(filename, 'w', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(data)
    print('Данные записаны!')
    return True


"""Создание функции по чтению файла employees_data формата CSV и добавлением в него столбца employee_id при выводе
для дальнейшей записи"""


def get_csv_data_employees(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        headers.insert(0, 'employee_id')
        data = []
        for row in reader:
            data.append(row)
    return headers, data


"""Создание функции записи измененного файла employees_data в формате CSV"""


def write_csv_data_employee(filename, headers, data):
    with open(filename, 'w', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)

        for id, row in enumerate(data, start=1):
            writer.writerow([id] + row)


"""Создание функции по заполнению таблицы customers_data данными из CSV-файла(output_customers_data.csv)"""


def fill_customers_data_table(cursor, filename, table_name):
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)

            COMMAND = f"""INSERT INTO {table_name} (customer_id, company_name, contract_name)
                                                    VALUES (?, ?, ?);"""
            for row in reader:
                if len == 3:
                    try:
                        cursor.execute(COMMAND, row)
                    except pyodbc.Error as e:
                        print(f"Ошибка при вставке строки {row}: {e}")
                else:
                    print(f"Некорректные данные пропущены: {row}")

        print(f"Данные из файла {filename} успешно добавлены в таблицу {table_name}.")
    except FileNotFoundError:
        print(f"Файл {filename} не найден.")
    except Exception as ex:
        print(f"Произошла ошибка: {ex}")


"""Создание функции по заполнению таблицы employees_data данными из CSV-файла(output_employees_data.csv)"""


def fill_employees_data_table(cursor, filename, table_name):
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)

            COMMAND = f"""INSERT INTO {table_name} (employee_id, first_name, last_name, title, birth_data, notes)
                          VALUES (?, ?, ?, ?, ?, ?);"""
            for row in reader:
                if len(row) == 6:
                    try:
                        cursor.execute(COMMAND, row)
                    except pyodbc.Error as e:
                        print(f"Ошибка при вставке строки {row}: {e}")
                else:
                    print(f"Некорректные данные пропущены: {row}")
        print(f"Данные из файла {filename} успешно добавлены в таблицу {table_name}.")
    except FileNotFoundError:
        print(f"Файл {filename} не найден.")
    except Exception as ex:
        print(f"Произошла ошибка: {ex}")


"""Создание функции по заполнению таблицы orders_data данными из CSV-файла(output_orders_data.csv)"""


def fill_orders_data_table(cursor, filename, table_name):
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)

            COMMAND = f"""INSERT INTO {table_name} (order_id, customer_id, employee_id, order_date, ship_city)
                                                    VALUES (?, ?, ?, ?, ?);"""

            for row in reader:
                if len(row) == 5:
                    try:
                        cursor.execute(COMMAND, row)
                    except pyodbc.Error as e:
                        print(f"Ошибка при вставке строки {row}: {e}")
                else:
                    print(f"Некорректные данные пропущены: {row}")
            print(f"Данные из файла {filename} успешно добавлены в таблицу {table_name}.")
    except FileNotFoundError:
        print(f"Файл {filename} не найден.")
    except Exception as ex:
        print(f"Произошла ошибка: {ex}")


if __name__ == '__main__':
    data_to_write = get_csv_data(r'files\customers_data.csv')
    print(get_csv_data(r'files\customers_data.csv'))
    print(write_csv_in_file(r'files\output_customers_data.csv', data_to_write))

    data_to_write = get_csv_data(r'files\orders_data.csv')
    print(get_csv_data(r'files\orders_data.csv'))
    print(write_csv_in_file(r'files\output_orders_data.csv', data_to_write))

    headers, data = get_csv_data_employees(r'files\employees_data.csv')
    print(headers)
    print(data)
    print(write_csv_data_employee(r'files\output_employees_data.csv', headers, data))
