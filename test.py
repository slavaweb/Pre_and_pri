import os
import pyodbc
import ntpath
import sys
from datetime import datetime
import pyexcel as pe

def get_file_list(dir_name):
    """Поулчить список файлов из папки
    """
    all_files = os.listdir(dir_name)
    file_list = []
    for file_name in all_files:
        extensions = ntpath.splitext(file_name)[1].upper()
        first_letter = file_name[0]
        if first_letter in ('~', '.'):
            continue
        elif extensions in FILE_EXENTIONS:
            file_list.append(file_name)
    return file_list


def insert_data(data, cnxn):
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    query = f"INSERT INTO [##pre_pri_from_pfr]\n"
    query += "VALUES\n("
    for i, dat in enumerate(data):
        query += f"'{data[i]}'"
        if i != len(list(data)) - 1:
            query += ", \n"
        else:
            query += "\n);"
    # print(query)
    cursor.execute(query)
    cnxn.commit()

# def exec_query(database, server, user, pwd, file_script, mode='create'):
#     cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
#                            "Server=" + server + ";"
#                            "Database=" + database + ";"
#                            "UID=" + user + ";"
#                            "PWD=" + pwd + "")
#     cursor = cnxn.cursor()
#     cursor.fast_executemany = True
#     file_script = file_script
#     fullpath = os.path.join(FOLDER_SCRIPTS, file_script)
#     query = ""
#     with open(fullpath, encoding='CP1251') as f:
#         for line in f:
#             query += line
#     if mode == 'execute':
#         result = cursor.execute(query)
#         return result
#     if mode == 'create':
#         cursor.commit()

FOLDER_SCRIPTS = 'script'
SQL_SERVER = '192.168.1.21'
SQL_DATABASE = 'kray'
SQL_USER = 'sa'
SQL_PWD = '137PfobJncnZoeh'
FILE_EXENTIONS = ('.XLSX', '.XLS')
XLS_IN = 'in'
XLS_OUT = 'out'
DATE_FILE = ''
XLS_SAMPLE = 'sample'


def func_it(file_name):
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=" + SQL_SERVER + ";"
                                               "Database=" + SQL_DATABASE + ";"
                                                                        "UID=" + SQL_USER + ";"
                                                                                        "PWD=" + SQL_PWD + "")
    cursor_1 = cnxn.cursor()
    cursor_1.fast_executemany = True
    file_script_1 = 'create_table1.sql'
    fullpath_1 = os.path.join(FOLDER_SCRIPTS, file_script_1)
    query_1 = ""
    with open(fullpath_1, encoding='CP1251') as f:
        for line in f:
            query_1 += line
    cursor_1.execute(query_1)
    cursor_1.commit()

    cursor_2 = cnxn.cursor()
    cursor_2.fast_executemany = True
    file_script_2 = 'create_table2.sql'
    fullpath_2 = os.path.join(FOLDER_SCRIPTS, file_script_2)
    query_2 = ""
    with open(fullpath_2, encoding='CP1251') as f:
        for line in f:
            query_2 += line

    cursor_2.execute(query_2)
    cursor_2.commit()

    full_file_name = os.path.join(XLS_IN, file_name)
    array = pe.get_array(file_name=full_file_name)
    # print('Импортирую данные базу...')
    for i, row in enumerate(array):
        if i == 3:
            DATE_FILE = row[1].strftime("%m.%d.%Y")
        if i < 8:
            continue
        ray = row[0]
        snils = row[1].replace('-', '').replace(' ', '')
        surname = row[2]
        firstname = row[3]
        middlename = row[4]
        dtr = row[5].strftime("%m.%d.%Y")
        sroks = row[6].strftime("%m.%d.%Y")
        oper = row[7]
        pri = row[8]
        dto = row[10].strftime("%m.%d.%Y")
        data = [ray, snils, surname, firstname, middlename, dtr, sroks, oper, pri, dto]
        insert_data(data, cnxn)

    file_sample = os.path.join(XLS_SAMPLE, 'sample.xlsx')
    file_out = f'{file_name}_сверено.xlsx'
    sheet = pe.get_sheet(file_name=file_sample)
    # print('Сохраняю результат в XLSX')

    cursor_3 = cnxn.cursor()
    cursor_3.fast_executemany = True
    file_script_3 = 'script1.sql'
    fullpath_3 = os.path.join(FOLDER_SCRIPTS, file_script_3)
    query_3 = ""
    with open(fullpath_3, encoding='CP1251') as f:
        for line in f:
            query_3 += line

    cursor_3.execute(query_3)
    for data in cursor_3:
        sheet.row += list(data)
    sheet.save_as(os.path.join(XLS_OUT, file_out))
    cursor_1.close()
    cursor_2.close()
    cursor_3.close()


file_list = get_file_list(XLS_IN)
for file in file_list:
    func_it(file)
    print(file, 'Готово')

print('сверка завершена')