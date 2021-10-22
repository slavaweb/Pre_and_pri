import pyodbc
import ntpath
import sys
from datetime import datetime
import shutil
from collections import defaultdict
import os
import pyexcel as pe
import pyexcel_xls
import pyexcel_xlsx
import pyexcel_io
import pyexcel_io.writers


class Sql:
    def __init__(self, database, server, user, pwd):
        self.cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                    "Server=" + server + ";"
                                    "Database=" + database + ";"
                                    "Trusted_Connection=yes;"
                                    "UID=" + user + ";"
                                    "PWD="+pwd+"")
        self.query = "-- {}\n\n-- Made in Python".format(datetime.now().strftime("%d/%m/%Y"))

    def create_table(self, column_name, table):
        cursor = self.cnxn.cursor()
        cursor.fast_executemany = True
        query = f"IF OBJECT_ID('{table}') IS NOT NULL DROP TABLE {table} \n"
        query += f"CREATE TABLE {table} (\n"
        for i, column in enumerate(column_name):
            query += f"\t[{column}] VARCHAR(255)"
            if i != len(list(column_name)) - 1:
                query += ", \n"
            else:
                query += "\n);"
        cursor.execute(query)
        self.cnxn.commit()
        self.query += ("\n\n-- create table\n" + query)

    def insert_data(self, data, table):
        cursor = self.cnxn.cursor()
        cursor.fast_executemany = True
        query = f"INSERT INTO [{table}]\n"
        query += "VALUES\n("
        for i, dat in enumerate(data):
            query += f"'{data[i]}'"
            if i != len(list(data)) - 1:
                query += ", \n"
            else:
                query += "\n);"
        # print(query)
        cursor.execute(query)
        self.cnxn.commit()
        self.query += ("\n\n-- insert data\n" + query)

    def main_join(self):
        cursor = self.cnxn.cursor()
        cursor.fast_executemany = True
        query = """
        DECLARE @status int, @pcstatus int, @mspstatus int
                    
        SET @status = ( SELECT TOP 1 A_ID FROM ESRN_SERV_STATUS WHERE A_STATUSCODE = 'act' ) -- не удалено
        SET @pcstatus = ( SELECT TOP 1 OUID FROM SPR_PC_STATUS WHERE A_CODE = 1 ) -- лд прекращено
        SET @mspstatus = ( SELECT TOP 1 A_ID FROM SPR_STATUS_PROCESS WHERE A_CODE = 100 ) 
                    
        INSERT INTO tmpRsdAll
        SELECT DISTINCT
        LEFT (pc.A_IDCODE, 3) AS [RAY]
        , pc.A_IDCODE AS [NKAR]
        , pc.OUID AS PC_OUID
        , pc.A_SNILS
        , rsd.A_STATUSPRIVELEGE
        , CONVERT(VARCHAR(10), ssp.STARTDATE, 104) AS STARTDATE
        , CONVERT(VARCHAR(10), ssp.A_LASTDATE, 104) AS A_LASTDATE
        
        FROM WM_PERSONAL_CARD pc 
        JOIN ESRN_SERV_SERV AS rsd ON rsd.A_PERSONOUID = pc.OUID
            JOIN SPR_NPD_MSP_CAT AS snmc ON snmc.A_ID = rsd.A_SERV
                AND ( snmc.A_STATUS = @status OR snmc.A_STATUS IS NULL )
            JOIN PPR_SERV AS ps ON ps.A_ID = snmc.A_MSP
                AND ( ps.A_STATUS = @status OR ps.A_STATUS IS NULL )
                AND ps.A_COD = 'rsd'
            LEFT JOIN (
                SELECT ROW_NUMBER() OVER ( PARTITION BY A_SERV ORDER BY STARTDATE DESC ) AS rowNum
                            , A_SERV
                            , STARTDATE
                            , A_LASTDATE
                        FROM SPR_SERV_PERIOD
                        WHERE A_STATUS = @status
                    ) AS ssp ON ssp.A_SERV = rsd.OUID
                    AND ssp.rowNum = 1
        WHERE pc.A_STATUS = @status AND pc.A_SNILS IS NOT NULL
        ORDER BY [NKAR] 
        """
        cursor.execute(query)
        cursor.commit()

        query = """
        DECLARE @status int, @pcstatus int, @mspstatus int
        SET @status = ( SELECT TOP 1 A_ID FROM ESRN_SERV_STATUS WHERE A_STATUSCODE = 'act' ) -- не удалено
        SET @pcstatus = ( SELECT TOP 1 OUID FROM SPR_PC_STATUS WHERE A_CODE = 1 ) -- лд прекращено
        SET @mspstatus = ( SELECT TOP 1 A_ID FROM SPR_STATUS_PROCESS WHERE A_CODE = 100 ) 
        SELECT DISTINCT COALESCE(rsd_new.RAY, rsd_old.RAY) AS RAY
        , pfr.[RAY]
        , pfr.[SNILS]
        , pfr.[SURNAME]
        , pfr.[FIRSTNAME]
        , pfr.[MIDDLENAME]
        , pfr.[DTR]
        , pfr.[SROKS]
        , pfr.[OPER]
        , pfr.[CAUSE]
        , pfr.[DATO]
        , ISNULL(rsd_new.msp, '') AS cur_msp
        , ISNULL(rsd_old_list.msp, '') AS old_msp 
        FROM pre_pri_from_pfr pfr
            LEFT JOIN (
                SELECT DISTINCT 
                ROW_NUMBER() OVER ( partition by A_SNILS ORDER BY STARTDATE DESC ) AS rowNum
                , RAY
                , A_SNILS
                , CONVERT(VARCHAR(10), STARTDATE, 104) + ' по ' + ISNULL (CONVERT(VARCHAR(10), A_LASTDATE, 104), 'бессрочно') AS msp
                FROM tmpRsdAll tmp 
                WHERE (tmp.A_LASTDATE >= GETDATE() OR tmp.A_LASTDATE IS NULL)
                ) rsd_new ON rsd_new.A_SNILS = pfr.SNILS
                AND rsd_new.rowNum = 1
                    
            LEFT JOIN (
                SELECT DISTINCT 
                ROW_NUMBER() OVER ( partition by A_SNILS ORDER BY STARTDATE DESC ) AS rowNum
                , RAY
                , A_SNILS
                , STARTDATE, A_LASTDATE 
                , CONVERT(VARCHAR(10), STARTDATE, 104) + ' по ' + ISNULL (CONVERT(VARCHAR(10), A_LASTDATE, 104), 'бессрочно') AS msp
                FROM tmpRsdAll tmp
                WHERE tmp.A_LASTDATE < GETDATE()
                    
                ) rsd_old ON rsd_old.A_SNILS = pfr.SNILS
                    AND rsd_old.rowNum = 1
                    
            OUTER APPLY (
            SELECT DISTINCT A_SNILS
                , ISNULL(STUFF((
                    SELECT DISTINCT CHAR(10) +'В ['+rsd1.RAY+'], c '+ ISNULL(CONVERT(VARCHAR(10), rsd1.STARTDATE, 104), 'безначально') + ' по ' 
                        + ISNULL (CONVERT(VARCHAR(10), rsd1.A_LASTDATE, 104), 'бессрочно') 
                    FROM tmpRsdAll rsd1
                    WHERE rsd1.A_SNILS = tmp.A_SNILS
                FOR XML PATH('')
                ),1,1,''),'') AS msp
                    
                FROM tmpRsdAll tmp
                WHERE tmp.A_LASTDATE < GETDATE()
                AND tmp.A_SNILS = pfr.SNILS
            ) rsd_old_list
        order by [SURNAME]
        """
        file_sample = os.path.join(XLS_SAMPLE, 'sample.xlsx')
        file_out = f'Сверка ПРЕ и ПРИ от {DATE_FILE}.xlsx'
        sheet = pe.get_sheet(file_name=file_sample)
        print('Сохраняю результат в XLSX')
        cursor.execute(query)
        for data in cursor:
            sheet.row += list(data)
        sheet.save_as(os.path.join(XLS_OUT, file_out))

    def drop_table(self, table):
        cursor = self.cnxn.cursor()
        cursor.fast_executemany = True
        query = f"IF OBJECT_ID('{table}') IS NOT NULL DROP TABLE {table}"
        cursor.execute(query)
        self.cnxn.commit()


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


SQL_SERVER = '192.168.1.21'
SQL_DATABASE = 'kray'
SQL_USER = 'sa'
SQL_PWD = '137PfobJncnZoeh'
FILE_EXENTIONS = ('.XLSX', '.XLS')
COLUMN_NAMESES = [['RAY', 'SNILS', 'SURNAME', 'FIRSTNAME', 'MIDDLENAME', 'DTR', 'SROKS', 'OPER', 'CAUSE', 'DATO'],
                ['RAY', 'NKAR', 'PC_OUID', 'A_SNILS', 'A_STATUSPRIVELEGE', 'STARTDATE', 'A_LASTDATE']]

TABLES = ['pre_pri_from_pfr', 'tmpRsdAll']
XLS_IN = 'in'
XLS_OUT = 'out'
XLS_SAMPLE = 'sample'
DATE_FILE = ''

sql = Sql(database=SQL_DATABASE, server=SQL_SERVER, user=SQL_USER, pwd=SQL_PWD)
sql.create_table(COLUMN_NAMESES[0], TABLES[0])
sql.create_table(COLUMN_NAMESES[1], TABLES[1])

file_list = get_file_list(XLS_IN)
full_file_name = os.path.join(XLS_IN, file_list[0])
array = pe.get_array(file_name=full_file_name)
sheets = pe.get_book(file_name=full_file_name)

print('Импортирую данные базу...')
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
    sql.insert_data(data, TABLES[0])

print('Приступаю к сверке')
sql.main_join()
print('Очистка от временных данных')
sql.drop_table(TABLES[0])
sql.drop_table(TABLES[1])
print('Готово!')


