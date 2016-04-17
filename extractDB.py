
import pyodbc,datetime,csv, xlsxwriter,argparse, os
from threading import Thread, Lock
from functools import wraps
from abc import abstractmethod
from time import  time
from queue import Queue




FETCH_SIZE = 20
MSSQL_INFO = {}
XLSX_MAX_LEN = 30
MSSQL_INFO["verbosity"] = False
NOTHREADS = 20
MSSQL_INFO["driver"] = "SQL Server"
MSSQL_INFO["PWD"] = None
MSSQL_INFO["schema"] = "dbo"
MSSQL_INFO["dbname"] = None
MSSQL_INFO["built-in dbs"] = ("master", "model", "msdb", "tempdb")
EXCLUDED_DBS = []

def create_connection(func):

    @wraps(func)
    def wrapper(*args, **kwargs):


        servername = MSSQL_INFO["servername"]

        drivername = MSSQL_INFO["driver"]
        if MSSQL_INFO["PWD"] and MSSQL_INFO["dbname"]:
            pwd = MSSQL_INFO["PWD"]
            databasename = MSSQL_INFO["dbname"]
            connection_str = 'DRIVER={'+drivername+'};SERVER='+servername+';\
                      DATABASE='+databasename+'PWD='+pwd+';\
                       timeout=5000; Trusted_Connection=Yes;\
                      CHARSET=UTF8'
        elif  MSSQL_INFO["PWD"] and not MSSQL_INFO["dbname"]:
            pwd = MSSQL_INFO["PWD"]
            connection_str = 'DRIVER={'+drivername+'};SERVER='+servername+'PWD='+pwd+';\
                       timeout=5000; Trusted_Connection=Yes;\
                      CHARSET=UTF8'

        elif not MSSQL_INFO["PWD"] and MSSQL_INFO["dbname"]:

            databasename = MSSQL_INFO["dbname"]

            connection_str = 'DRIVER={'+drivername+'};SERVER='+servername+';\
                      DATABASE='+databasename+';\
                       timeout=5000; Trusted_Connection=Yes;\
                      CHARSET=UTF8'

        else:

            connection_str = 'DRIVER={'+drivername+'};SERVER='+servername+';\
                       timeout=5000; Trusted_Connection=Yes;\
                      CHARSET=UTF8'
        try:
            if MSSQL_INFO["verbosity"]:
                print ("opening connection to Database")
            cnxn = pyodbc.connect(connection_str)


            cursor = cnxn.cursor()

            retval = func(cursor, *args)
            if MSSQL_INFO["verbosity"]:
                print ("closed connection to Database")
            return retval

        except pyodbc.DatabaseError:
            print ("Connection Error! See below")
            raise

    return wrapper








@create_connection
def retrieve_data_iter(cursor, *args):
        tablename = args[0]
        cursor = cursor.execute(u"SELECT * FROM "+tablename)
        while True:
            rows = cursor.fetchmany(FETCH_SIZE)
            if not rows:
                break
            for row in rows:
                yield row

@create_connection
def retrieve_database_names(cursor):
    dbs = cursor.execute("SELECT name from sys.DATABASES").fetchall()
    return list(db[0] for db in dbs if not db[0] in MSSQL_INFO["built-in dbs"])

@create_connection
def retrieve_table_names(cursor):
    schema = MSSQL_INFO["schema"]
    tables = cursor.tables(schema=schema, tableType="TABLE").fetchall()
    return list(table[2] for table in tables)


@create_connection
def retrieve_column_names(cursor, *args):
         schema = MSSQL_INFO["schema"]
         tablename = args[0]
         columns = cursor.columns(table=tablename, schema=schema)
         return list(column.column_name for column in columns)



def create_dirs(database):
        """try to create necessary folders existing db folders
        will be treated as they have been parsed"""
        try:
            if not os.path.exists(database):
                os.makedirs(database)
            else:
                EXCLUDED_DBS.append(database)
            os.makedirs(database+"/xlsx")
            os.makedirs(database+"/csv")
        except OSError as e:
            print (e)
            pass

class Writer:
    @abstractmethod
    def write(self, colnames, data):
        """Strategy for write interface"""
        pass




class CSVWriter(Writer):
    def __init__(self, dbname, tablename):
        self.filepath = dbname+"/csv/"+tablename+".csv"



    def write(self,  colnames, data):
        with open(self.filepath, 'w+', newline='', encoding="utf-8") as csvfile:
            tablewriter = csv.writer(csvfile, delimiter=',')
            tablewriter.writerow(colnames)
            if MSSQL_INFO["verbosity"]:
                print ("writing to csv ", len(data), self.filepath)
            for row in data:
                csv_list = []
                for cid, data in enumerate(row):
                    if isinstance(data, datetime.datetime):
                        csv_list.append(data.isoformat(' '))

                    else:
                        csv_list.append(data)
                tablewriter.writerow(csv_list)


class XLSXWriter(Writer):
    def __init__(self, dbname, tablename):
        self.filepath = dbname+"/xlsx/"+tablename+".xlsx"
        self.tablename = tablename

    def write(self, colnames, data):

        workbook = xlsxwriter.Workbook(self.filepath)
        dateformat = workbook.add_format({'num_format': 'dd/mm/yyyy H:M:S'})
        worksheet = workbook.add_worksheet(self.tablename[:XLSX_MAX_LEN])  # max length of xlsx sheet
        worksheet.write_row(0, 0, colnames)
        for rid, row in enumerate(data):

            for cid, col in enumerate(row):
                if isinstance(col, datetime.datetime):

                    worksheet.write(rid + 1, cid, col, dateformat)
                elif isinstance(col, bytearray):  # encoding issues

                    try:
                        worksheet.write_string(rid + 1, cid, col.decode('utf-8'))
                    except UnicodeDecodeError:
                        worksheet.write_string(rid + 1, cid, "decoding error")
                else:
                    worksheet.write(rid + 1, cid, col)

        workbook.close()



class Database:
    def __init__(self, dbname):
        self.dbname = dbname
        self.tables = []



    @property
    def noftables(self):
        return len(self.tables)



    def find_tables(self):
        print ("Retrieving table names for db", self.dbname)
        self.tables = retrieve_table_names()







class Table:
    def __init__(self,  tablename, colnames):
        self.tablename = tablename
        self.colnames = colnames
        self.data = []


     


    def get_data(self):
        """parse data from table rows"""
        for rid, row in enumerate(retrieve_data_iter(self.tablename, dbname)):
            self.data.append(row)
        if MSSQL_INFO["verbosity"]:
            print ("Data for ", len(self.data), self.tablename)






class Locker(object):
    def __init__(self):
        self.lock = Lock()
        self.__elapsed = 0.0
        self.__start_time = time()

    def decrement(self):
        self.remaining -= 1

    def total_time(self):
        self.__elapsed += time()-self.__start_time

    def reset(self, noftables):
        self.remaining = noftables

    @property
    def elapsed(self):
        return format(self.__elapsed, '4.2f')





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("server",help="Specify Server Name")
    parser.add_argument("--db", help="Specify database name")
    parser.add_argument("--threaded",help="default non threaded",type=bool)
    parser.add_argument("--verbose", help="enhanced output")
    parser.add_argument("--fetchsize", help="how many records will be fetched for each query",type=int)
    args = parser.parse_args()


    MSSQL_INFO["servername"] = args.server
    if args.verbose:
        MSSQL_INFO["verbosity"] = True



    if args.fetchsize:
        FETCH_SIZE = args.fetchsize


    def process_table_non_threaded(tname):

        writers = []
        if MSSQL_INFO["verbosity"]:
            print ("Started parsing table {0}".format(tname))

        colnames = retrieve_column_names(tname)
        t = Table(tname, colnames)
        t.get_data()

        writers.append(XLSXWriter(db.dbname, tname))
        writers.append(CSVWriter(db.dbname, tname))
        if MSSQL_INFO["verbosity"]:
            print ("Before writing ", len(t.data))
        [w.write(t.colnames, t.data) for w in writers]



        lck.decrement()
        lck.total_time()
        print ("Finished parsing table {0} remaining {1} tables out of {2} in {3} secs so far".
               format(tname, lck.remaining, db.noftables, lck.elapsed))


    def process_table_threaded():
        """consumer for queue"""
        while True:#consume queue until threads have finished
            tname = queue.get()
            writers = []
            #print ("Started parsing table {0}".format(tname))
            colnames = retrieve_column_names( tname)
            t = Table(tname, colnames)
            t.get_data()

            writers.append(XLSXWriter(db.dbname, tname))
            writers.append(CSVWriter(db.dbname, tname))
            if MSSQL_INFO["verbosity"]:
                print ("Before writing ", len(t.data))
            [w.write(t.colnames, t.data) for w in writers]



            lck.decrement()
            lck.total_time()
            print ("Finished table {0} remaining to be processed {1} from tables out of {2} in queue  {3} "
                   " {4} secs so far".
               format(tname, lck.remaining, queue.qsize(), db.noftables, lck.elapsed))


            queue.task_done()
            if lck.remaining == 0:#not terminating always
                queue.task_done()



    if args.db:
        databases = [args.db]
    else:
        databases = retrieve_database_names()
    lck = Locker()

    for dbname in databases:
        db = Database(dbname)
        MSSQL_INFO["dbname"] = dbname
        db.find_tables()
        create_dirs(db.dbname)
        if db.dbname not in EXCLUDED_DBS:

            lck.reset(db.noftables)

            if args.threaded:
                queue = Queue(db.noftables)
                for table in (sorted(db.tables)):
                    queue.put(table)
                for i in range(NOTHREADS):
                    thread = Thread(target=process_table_threaded)
                    thread.setDaemon(True)
                    thread.start()

                queue.join()
                continue



            else:
                for table in (sorted(db.tables)):
                    process_table_non_threaded(table)


