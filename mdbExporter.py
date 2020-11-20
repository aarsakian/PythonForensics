import os
import sys
from datetime import datetime
import logging
import subprocess
import shlex

import xlsxwriter

import pyodbc

from functools import wraps


FETCH_SIZE = 100

ALLOWED_FORMATS = (".accdb", ".mdb", ".mde", ".mpp", ".mpe")

table_data = {}


logfilename = str(datetime.now()).replace(":","_")+".log"
logging.basicConfig(filename=logfilename,
                    filemode='a',
                    format='%(asctime)s,  %(name)s %(levelname)s %(message)s',
                    datefmt='%x %H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)



class Table:
    def __init__(self, tablename, fullpath):
        self.tablename = tablename
        self.fullpath = fullpath
        self.colnames = []

    @property
    def worksheet_name(self):
        worksheet_name = self.tablename[0:28]
        if len(self.tablename) > 28:
            worksheet_name += "..."

        return worksheet_name
        
   
    
    def get_column_names(self, collector):
        self.colnames = collector.retrieve_column_names(self.fullpath, self.tablename)

    def get_data(self, collector):
        """parse data from table rows"""
        data = [row for rid, row in enumerate(collector.retrieve_data_iter(self.fullpath, self.tablename))]

        if data and isinstance(data[0], str):
            if data[0].startswith("Unicode error") or data[0].startswith("No read permission"):
                data = [u"Σφάλμα κατά την εξαγωγή!"]*len(self.colnames)
        
        table_data[self.worksheet_name] = (self.colnames, data)



def write_to_xlsx(fullpath):
        xlsxfname = fullpath[:-4] + ".xlsx"
        logger.info("writing to xlsx file{}".format(xlsxfname))
        workbook = xlsxwriter.Workbook(xlsxfname)
        for worksheet_name, (header, contents) in table_data.items():
            worksheet = workbook.add_worksheet(worksheet_name)
            date_format = '%d/%m/%y %H:%M:%S'

            xls_date_format = workbook.add_format({'num_format': 'd/m/y H:M:S'})
            worksheet.write_row(0, 0, header)
            for row_idx, row in enumerate(contents):
                worksheet.write_row(row_idx+1, 0, row)
        workbook.close()



def find_mdb_files(folder):
    for root, dirs, files in os.walk(folder):
        dirs.sort()
        for file in sorted(files):
            if os.path.splitext(file)[-1].lower() in ALLOWED_FORMATS:
                 yield file, root


def create_connection(func):

    @wraps(func)
    def wrapper(*args, **kwargs):

        MDB = args[0] # fullpath to MDB file
        DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
      
        conn = pyodbc.connect('DRIVER={};DBQ={}'.format(DRV, MDB))
        conn.setencoding(encoding='utf-8', ctype=pyodbc.SQL_CHAR)
        conn.add_output_converter(13888, handle_unknown_type)
        conn.add_output_converter(15912, handle_unknown_type)
        conn.add_output_converter(108, handle_unknown_type)
        conn.add_output_converter(0, handle_unknown_type)
        cursor = conn.cursor()

        retval = func(cursor, args)
        return retval

    return wrapper


class MDBCollector:
     def retrieve_table_names(self, database_name):
         cmd = 'mdb-tables'
         delimiter = "-d ,"
         tables = subprocess.check_output([cmd, delimiter, database_name])
         
         return [table_info.lstrip().rstrip() for table_info in tables.decode("utf-8").split(",")][:-1]
         
     def retrieve_column_names(self, database_name, table_name):
         cmd = 'mdb-export'
         row_delimiter = "-R ,"
         col_delimiter = "-d |"
         column_names = subprocess.check_output([cmd, row_delimiter, col_delimiter,
         database_name, table_name])
         return [column_name for column_name in column_names.decode("utf-8").split(",")[0].split("|")]
         
         
     def retrieve_data_iter(self, database_name, table_name):
         cmd = 'mdb-export'
         row_delimiter = "-R ***"
         suppress_header = '-H'
         col_delimiter = "-d |"
         data = subprocess.check_output([cmd, row_delimiter, col_delimiter,
            suppress_header ,database_name, table_name])
        
         for row in data.decode("utf-8").split("***"):
             yield [column for column in row.split("|")]

class ODBCCollector:

     def handle_unknown_type(value):
       logging.info("unsupported type {}".format(value))
       print ("VAL "+value)


     @create_connection
     def retrieve_table_names(cursor, *args):
         return [table_info[2] for table_info in cursor.tables(tableType='TABLE')]

     @create_connection
     def retrieve_column_names(cursor, *args):

        tablename = args[0][1]
        colnames = []
        try:
           for c in cursor.columns(table=tablename):
               colnames.append(c.column_name)
        except UnicodeDecodeError as e:
           logger.error("Unicode Error when retrieving column names for table {}".format(tablename))
           print ("Unicode Error when retrieving colunms for table {}".format(tablename))
        return colnames



     @create_connection
     def retrieve_data_iter(cursor, *args):

        tablename = args[0][1]
        try:
            cursor = cursor.execute("SELECT * FROM [{}]".format(tablename))

            while True:

                rows = cursor.fetchmany(FETCH_SIZE)

                if not rows:
                    cursor.close()
                    del cursor
                    break
                for row in rows:
                    yield row


        except UnicodeDecodeError:
            print ('Unicode error {}'.format(tablename))
            logger.error("Unicode error retrieving data from table  {}".format(tablename))
            yield 'Unicode error {}'.format(tablename)

        except pyodbc.ProgrammingError as e:
            print('{} {}'.format(e, tablename))
            logger.error(" Permission error when retrieving data from table {}".format(tablename))
            yield 'No read permission on table {}'.format(tablename)


if __name__ == "__main__":
    logger.info("Extraction of MS Access to xlsx initiated")
    file_entries = {}
    
    collector = MDBCollector()
    for file, root in find_mdb_files(sys.argv[1]):
        fullpath = os.path.join(root,file)
        xlsxfname = fullpath[:-4] + ".xlsx"
        print ("processing file {}".format(fullpath))
        logger.info("processing file {}".format(fullpath))

        if os.path.isfile(xlsxfname):
            print ("skipped file {} already processed".format(fullpath))
            logger.info("skipped file {} already processed".format(fullpath))
            continue

        table_names = collector.retrieve_table_names(fullpath)
        for table_info in table_names:
            logger.info("retrieving data from table {}".format(table_info))
            t = Table(table_info, fullpath)
            t.get_column_names(collector)
            t.get_data(collector)
            
            
        write_to_xlsx(fullpath)
        



