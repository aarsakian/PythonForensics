
import pyodbc,csv, xlsxwriter,argparse, os, gc
from threading import Thread, Lock
from functools import wraps
from abc import abstractmethod
from time import  time
from queue import Queue
from memory_profiler import profile
import logging, tracemalloc
from datetime import datetime

FETCH_SIZE = 20
MSSQL_INFO = {}
XLSX_MAX_LEN = 30
TABLE = None
SKIP_TABLES = []
MSSQL_INFO["verbosity"] = False
NOTHREADS = 20
MSSQL_INFO["driver"] = "SQL Server"
MSSQL_INFO["PWD"] = None
MSSQL_INFO["schema"] = "dbo"
MSSQL_INFO["dbname"] = None
MSSQL_INFO["built-in dbs"] = ("master", "model", "msdb", "tempdb")
EXCLUDED_DBS = []

logfilename = "db"+str(datetime.now()).replace(":","_")+".log"
logging.basicConfig(filename=logfilename,
                    filemode='a',
                    format='%(asctime)s,  %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)


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
       
        cursor = cursor.execute("SELECT * FROM ["+tablename+"]")
        while True:
            rows = cursor.fetchmany(FETCH_SIZE)

            if not rows:
                cursor.close()
                del cursor
                gc.collect()
                break
            for row in rows:
                yield row

@create_connection
def retrieve_database_names(cursor):
    dbs = cursor.execute("SELECT name from sys.DATABASES").fetchall()
    cursor.close()
    del cursor
    gc.collect()
    logger.info("retrieving databases")
    return list(db[0] for db in dbs if not db[0] in MSSQL_INFO["built-in dbs"])

@create_connection
def retrieve_table_names(cursor):
    schema = MSSQL_INFO["schema"]
    logger.info("retrieving schema")
    tables = cursor.tables(schema=schema, tableType="TABLE").fetchall()
    cursor.close()
    del cursor


    gc.collect()
    
    if TABLE:
        return list(table[2] for table in tables if table[2] == TABLE)
    elif SKIP_TABLES:
        return list(table[2] for table in tables if table[2] not in SKIP_TABLES)
    else:
        return list(table[2] for table in tables)


@create_connection
def retrieve_column_names(cursor, *args):
         schema = MSSQL_INFO["schema"]
         tablename = args[0]
         logger.info("retrieving columns for table %s",tablename)
         columns = cursor.columns(table=tablename, schema=schema)
         colnames =  list(column.column_name for column in columns)
         cursor.close()
         del cursor
         gc.collect()
         return colnames



def create_dirs(database):
    """try to create necessary folders existing db folders
        will be treated as they have been parsed"""
		
    os.makedirs(database, exist_ok = True)
    os.makedirs(database+"/xlsx", exist_ok = True)
    os.makedirs(database+"/csv", exist_ok = True)
          
       
class Writer:

    @abstractmethod
    def write_header(self, colnames):
        pass

    @abstractmethod
    def write(self, data):
        """Strategy for write interface"""
        pass

    @abstractmethod
    def write_row(self, data):
        """strategy when we desire to be memory efficient write in steps"""
        pass
    
    def file_exists(self):
        return os.path.exists(self.filepath)



class CSVWriter(Writer):
    def __init__(self, dbname, tablename):
        self.filepath = os.path.join(dbname, "csv", tablename+".csv")
        logger.info("writing to %s", self.filepath)

    def write_header(self, colnames):
        with open(self.filepath, 'w+', newline='', encoding="utf-8") as csvfile:
            tablewriter = csv.writer(csvfile, delimiter=',')
            tablewriter.writerow(colnames)

    def _create_csv_list(self, row):
        csv_list = []
        for cid, data in enumerate(row):
            if isinstance(data, datetime):
                csv_list.append(data.isoformat(' '))
            else:
                csv_list.append(data)
        return csv_list

    def write(self, data):
        with open(self.filepath, 'a+', newline='', encoding="utf-8") as csvfile:
            tablewriter = csv.writer(csvfile, delimiter=',')
            if MSSQL_INFO["verbosity"]:
                print ("writing to csv ", len(data), self.filepath)
            for row in data:
                csv_list = self._create_csv_list(row)
                tablewriter.writerow(csv_list)

    def write_row(self, row):
        
        if MSSQL_INFO["verbosity"]:
            print ("file opened ", self.filepath)
        with open(self.filepath, 'a+', newline='', encoding="utf-8") as csvfile:
            tablewriter = csv.writer(csvfile, delimiter=',')
            csv_list = self._create_csv_list(row)
            
            tablewriter.writerow(csv_list)
            time2 = tracemalloc.take_snapshot()
       
        if MSSQL_INFO["verbosity"]:
            print ("file closed ", self.filepath)
            top_stats = time2.compare_to(self.time1, 'lineno')
            for stat in top_stats[10:]:
                print(stat)



class XLSXWriter(Writer):
    def __init__(self, dbname, tablename):
        self.filepath = os.path.join(dbname,"xlsx", tablename+".xlsx")
        self.workbook = xlsxwriter.Workbook(self.filepath)
        self.dateformat = self.workbook.add_format({'num_format': 'dd/mm/yyyy H:M:S'})
        self.worksheet = self.workbook.add_worksheet(tablename[:XLSX_MAX_LEN])  # max length of xlsx sheet
        self.rowid = 0
        logger.info("writing to %s", self.filepath)
    
    def write_header(self, colnames):
        self.worksheet.write_row(0, 0, colnames)


    def write(self,  data):
        
        for rid, row in enumerate(data):
            self.write_row(row)

        self.workbook.close()


    def write_row(self, data):
         
            for cid, col in enumerate(data):
                if isinstance(col, datetime):

                    self.worksheet.write(self.rowid + 1, cid, col, self.dateformat)
                elif isinstance(col, bytearray) or isinstance(col, bytes):  # encoding issues

                    try:
                        self.worksheet.write_string(self.rowid + 1, cid, col.decode('utf-8'))
                    except UnicodeDecodeError:
                        self.worksheet.write_string(self.rowid + 1, cid, "decoding error")
                else:
                    self.worksheet.write(self.rowid + 1, cid, col)


            self.rowid += 1



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
        
    def get_data(self):
        """parse data from table rows"""
        if MSSQL_INFO["verbosity"]:
            print ("Data for ", self.tablename)
        return [row for rid, row in enumerate(retrieve_data_iter(self.tablename, dbname))]
          
       






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
    parser.add_argument("--table", help="extract only a table from a database")
    parser.add_argument("--skiptables",  help="extract all tables from a database except the ones specified need to be comma seperated!")
    parser.add_argument("--fetchsize", help="how many records will be fetched for each query", type=int)
    parser.add_argument("--memory", help="reduce memory footprint", type=bool)
    
    args = parser.parse_args()


    MSSQL_INFO["servername"] = args.server #'WIN-PU4LF8SP8JU\SQLEXPRESS'
	
    if args.verbose:
        MSSQL_INFO["verbosity"] = True


    if args.db and args.table:
        TABLE = args.table
    elif args.table and not args.db:
        print("Please select a database first")
        exit(0)
	
    if args.db and args.skiptables:
        SKIP_TABLES = args.skiptables.split(',')
        logging.info("skipped tables {}".format(SKIP_TABLES))
    elif args.skiptables and not args.db:
        print("Please select a database first")
        exit(0)

    if args.fetchsize:
        FETCH_SIZE = args.fetchsize


    def process_table_non_threaded(tname):

        writers = []
        if MSSQL_INFO["verbosity"]:
            print ("Started parsing table {0}".format(tname))
        logging.info("Started parsing table {0}".format(tname))
        colnames = retrieve_column_names(tname)
        t = Table(tname, colnames)


        writers.append(XLSXWriter(db.dbname, tname))
        writers.append(CSVWriter(db.dbname, tname))
        [w.write_header(t.colnames)  for w in writers]
        if args.memory:

            for rid, row in enumerate(retrieve_data_iter(tname, dbname)):
                [w.write_row(row) for w in writers]
        else:
            
            if MSSQL_INFO["verbosity"]:
                print ("Before writing ")
            [w.write(t.get_data()) for w in writers]



        lck.decrement()
        lck.total_time()
        logging.info("Finished parsing table {0} remaining {1} tables out of {2} in {3} secs so far".
               format(tname, lck.remaining, db.noftables, lck.elapsed))
        if MSSQL_INFO["verbosity"]:
           print ("Finished parsing table {0} remaining {1} tables out of {2} in {3} secs so far".
               format(tname, lck.remaining, db.noftables, lck.elapsed))


    def process_table_threaded():
        """consumer for queue"""
        while True:#consume queue until threads have finished
            tname = queue.get()
            writers = []

            logging.info("Started parsing table {0}".format(tname))
            colnames = retrieve_column_names( tname)
            t = Table(tname, colnames)

            writers.append(XLSXWriter(db.dbname, tname))
            writers.append(CSVWriter(db.dbname, tname))
            [w.write_header(t.colnames)  for w in writers]
            if args.memory:
                 for rid, row in enumerate(retrieve_data_iter(tname, dbname)):
                    [w.write_row(row) for w in writers]
            else:
               
                if MSSQL_INFO["verbosity"]:
                    print ("Before writing ")
                [w.write(t.get_data()) for w in writers]

            if MSSQL_INFO["verbosity"]:
                print ("Before writing ")
            [w.write(t.get_data()) for w in writers]



            lck.decrement()
            lck.total_time()
            if MSSQL_INFO["verbosity"]:
                print ("Finished table {0} remaining to be processed {1} from tables out of {2} in queue  {3} "
                   " {4} secs so far".
               format(tname, lck.remaining, queue.qsize(), db.noftables, lck.elapsed))
            logging.info("Finished table {0} remaining to be processed {1} from tables out of {2} in queue  {3} "
                   " {4} secs so far".
               format(tname, lck.remaining, queue.qsize(), db.noftables, lck.elapsed))


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


