import fdb
import csv
import sys
import logging
import os
import xlsxwriter
import shutil
from collections import namedtuple
from functools import wraps
from datetime import datetime

DATABASE_PATH = "D:\PATH\TO\FIREBIRDFILE.FDB"
SELECTED_TABLE = None

XLSX_MAX_LEN = 30


logfilename = "db"+str(datetime.now()).replace(":","_")+".log"
logging.basicConfig(filename=logfilename,
                    filemode='a',
                    format='%(asctime)s,  %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)



def create_dirs(database):
    """try to create necessary folders existing db folders
        will be treated as they have been parsed"""
    shutil.rmtree(database, ignore_errors=True)
    os.makedirs(database, exist_ok = True)
    os.makedirs(database+"/xlsx", exist_ok = True)
    os.makedirs(database+"/csv", exist_ok = True)


def create_connection(func):

    @wraps(func)
    def wrapper(*args, **kwargs):

        cnxn = fdb.connect(host='localhost', database=DATABASE_PATH, user='sysdba', password='masterkey')

        cursor = cnxn.cursor()

        return func(cursor, cnxn, *args)
           
    return wrapper 


@create_connection
def get_tables(cur, cnxn):
    logger.info("retrieving table names")
    Table = namedtuple('Table', ['name', 'description'])
    return [Table(t.name, t.description) for t in cnxn.schema.tables]


@create_connection
def get_column_names(cur, cnxn, table_name):
    table = cnxn.schema.get_table(table_name)
    return [col.name for col in table.columns]


@create_connection
def extract_data_from_table(cur, cnxn, table_name):
   
    cur.execute("select * from {}".format(table_name))
  
    while True:
       row = cur.fetchone()
       if not row:
           
           break
       else:
           yield row


def write_to_csv(csv_name, row_data):
 
   with open(csv_name, 'a+', newline='', encoding="utf-8") as csvfile:
            tablewriter = csv.writer(csvfile, delimiter=',')
            
            tablewriter.writerow(row_data)






def write_to_xlsx(filepath, tablename, data):
    logging.info("writing to {} from {}".format(filepath, tablename))
    workbook = xlsxwriter.Workbook(filepath)
    dateformat = workbook.add_format({'num_format': 'dd/mm/yyyy H:M:S'})
    worksheet = workbook.add_worksheet(tablename[:XLSX_MAX_LEN])  # max length of xlsx sheet
    [write_row(worksheet, rowdata, rownum, dateformat) for (rownum, rowdata) in enumerate(data)]
    
    workbook.close()
    logging.info("finished writing to {} from {}".format(filepath, tablename))


def write_row(worksheet, data, rownum, dateformat):
   
    for cid, col in enumerate(data):
       
        if isinstance(col, datetime):

            worksheet.write(rownum, cid, col, dateformat)
        elif isinstance(col, bytearray) or isinstance(col, bytes):  # encoding issues

            try:
                worksheet.write_string(rownum, cid, col.decode('utf-8'))
            except UnicodeDecodeError:
                worksheet.write_string(rownum, cid, "decoding error")
        else:
         
            worksheet.write(rownum, cid, col)






def extract_data(tables, selected_table=None):
    for table in sorted(tables):
        if selected_table and table.name != selected_table:
            logging.info("skipped table {}".format(table.name))
            continue
        
        print ('extracting data from table {}'.format(table.name))
        full_csv_name = os.path.join(os.path.basename(DATABASE_PATH).split(".")[0], "csv", table.name+".csv")
        full_xslx_name = os.path.join(os.path.basename(DATABASE_PATH).split(".")[0], "xlsx", table.name+".xlsx")
        
        logging.info(" extracting column names from table {}".format(table.name))

        # write headers
        header = get_column_names(table.name)
    
        #logging.info("started extracting and writing data to csv and xlsx from table {}".format(table.name))
        
        data =[row_data for row_data in extract_data_from_table(table.name)]

        write_to_csv(full_csv_name, header)
        [write_to_csv(full_csv_name, row_data)  for  row_data in data]

        xlsx_data = []
        xlsx_data.append([table.description]) 
        xlsx_data.append(header)
        xlsx_data.extend(data)
        write_to_xlsx(full_xslx_name, table.name, xlsx_data)
            
       # logging.info("finished extracting and writing data to csv  and xlsx {} from table {} number of rows {}".format(full_csv_name, table.name, len(data)))
        print("finished extracting data from table {}".format(table.name))


def check_for_missing_tables(tables):
    path = os.listdir(os.path.join(os.path.basename(DATABASE_PATH).split(".")[0], "xlsx"))
    xlsxfiles = set([xlsxfile.split(".")[0] for xlsxfile in sorted(path)])
    tables =set([table.name for table in sorted(tables)])
    return set(tables)-xlsxfiles


if __name__ == "__main__":
   
    if len(sys.argv)>2:
        SELECTED_TABLE = sys.argv[2]
       
    if len(sys.argv)>1:
        DATABASE_PATH = sys.argv[1]
        
    create_dirs(DATABASE_PATH.split("\\")[-1].split(".")[0])
    tables = get_tables()
    
    logging.info("total number of tables to be extracted {}".format(len(tables)))
    extract_data(tables, SELECTED_TABLE)
    missing_tables = check_for_missing_tables(tables)
    logging.info("missing tables {}".format(missing_tables))
    print(missing_tables)
    
    print("Completed!")
            
