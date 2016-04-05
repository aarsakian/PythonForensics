import pyodbc,datetime,csv, xlsxwriter,argparse, os
from threading import Thread, Lock
from functools import wraps
from abc import abstractmethod
from time import sleep, time
from queue import Queue




FETCH_SIZE = 20
MSSQL_INFO = {}
XLSX_MAX_LEN = 30
MSSQL_INFO["verbosity"] = False
NOTHREADS = 20

def create_connection(func):

    @wraps(func)
    def wrapper(*args, **kwargs):

        databasename = MSSQL_INFO["databasename"]
        servername = MSSQL_INFO["servername"]

        try:
            if MSSQL_INFO["verbosity"]:
                print ("opening connection to Database")
            cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+servername+';\
                      DATABASE='+databasename+';\
                       timeout=5000; Trusted_Connection=Yes;\
                      CHARSET=UTF8')
            cursor = cnxn.cursor()
            retval = func(cursor, *args)
            if MSSQL_INFO["verbosity"]:
                print ("closed connection to Database")
            return retval

        except pyodbc.DatabaseError:
            print ("Connection Error! See below")
            raise
            exit
    return wrapper

@create_connection
def find_tables(cursor):
    tables = cursor.tables(schema="dbo", tableType="TABLE").fetchall()
    return list(table.table_name for table in tables)





def create_dirs(database, type_dir):
    try:
        os.makedirs(database+"/"+type_dir)
       
    except OSError as e: 
        print (e)
        pass
    return database+"/"+type_dir+"/"




@create_connection
def retrieve_data_iter(cursor, *args):
        tablename = args[0]
        cursor = cursor.execute(u"SELECT * FROM ["+tablename+"]")
        while True:
            rows = cursor.fetchmany(FETCH_SIZE)
            if not rows:
                break
            for row in rows:
                yield row




@create_connection
def retrieve_column_names(cursor, *args):
         tablename = args[0]
         columns = cursor.columns(table=tablename, schema="dbo")
         return list(column.column_name for column in columns)


class Writer:



    @abstractmethod
    def write(self, colnames, data):
        """Strategy for write interface"""
        pass


class CSVWriter(Writer):
    def __init__(self, path, tablename):
        self.filepath = path+tablename+".csv"


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
    def __init__(self, path, tablename):
        self.filepath = path+tablename+".xlsx"
        self.tablename = tablename

    def write(self, colnames, data):

        workbook = xlsxwriter.Workbook(self.filepath)
        dateformat = workbook.add_format({'num_format': 'dd/mm/yyyy h:m:S'})
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





class Table:
    def __init__(self,  tablename, colnames):
        self.tablename = tablename
        self.colnames = colnames
        self.data = []


     


    def get_data(self):
        """parse data from table rows"""
        for rid, row in enumerate(retrieve_data_iter(self.tablename)):
            self.data.append(row)
        if MSSQL_INFO["verbosity"]:
            print ("Data for ", len(self.data), self.tablename)






class Locker(object):
    def __init__(self, noftables):
        self.lock = Lock()
        self.remaining = noftables
        self.__elapsed = 0.0
        self.__start_time = time()

    def decrement(self):
        self.remaining -= 1

    def total_time(self):
        self.__elapsed += time()-self.__start_time

    @property
    def elapsed(self):
        return format(self.__elapsed, '4.2f')





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("server",help="Specify Server Name")
    parser.add_argument("db", help="Specify database name")
    parser.add_argument("--threaded",help="default non threaded",type=bool)
    parser.add_argument("--verbose", help="enhanced output")
    parser.add_argument("--fetchsize", help="how many records will be fetched for each query",type=int)
    args = parser.parse_args()

    MSSQL_INFO["databasename"] = args.db
    MSSQL_INFO["servername"] = args.server
    if args.verbose:
        MSSQL_INFO["verbosity"] = True


    tables = find_tables()

    if args.fetchsize:
        FETCH_SIZE = args.fetchsize

    paths=[]
    paths.append(create_dirs(args.db, "csv"))
    paths.append(create_dirs(args.db, "xlsx"))
    noftables = len(tables)



    def process_table_non_threaded(tname):

        writers = []
        print ("Started parsing table {0}".format(tname))
        colnames = retrieve_column_names(tname)
        t = Table(tname, colnames)
        t.get_data()

        writers.append(XLSXWriter(paths[1], tname))
        writers.append(CSVWriter(paths[0], tname))
        if MSSQL_INFO["verbosity"]:
            print ("Before writing ", len(t.data))
        [w.write(t.colnames, t.data) for w in writers]



        lck.decrement()
        lck.total_time()
        print ("Finished parsing table {0} remaining {1} tables out of {2} in {3} secs so far".
               format(tname, lck.remaining, noftables, lck.elapsed))


    def process_table_threaded():
        """consumer for queue"""
        while True:#consume queue until threads have finished
            tname = queue.get()

            writers = []
            #print ("Started parsing table {0}".format(tname))
            colnames = retrieve_column_names(tname)
            t = Table(tname, colnames)
            t.get_data()

            writers.append(XLSXWriter(paths[1], tname))
            writers.append(CSVWriter(paths[0], tname))
            if MSSQL_INFO["verbosity"]:
                print ("Before writing ", len(t.data))
            [w.write(t.colnames, t.data) for w in writers]



            lck.decrement()
            lck.total_time()
            print ("Finished table {0} remaining to be processed {1} from tables out of {2} in queue  {3} "
                   " {4} secs so far".
               format(tname, lck.remaining, queue.qsize(), noftables, lck.elapsed))
            queue.task_done()

    lck = Locker(noftables)


    if args.threaded:
        queue = Queue(noftables)

        for i in range(NOTHREADS):

            thread = Thread(target=process_table_threaded)
          #  sleep(0.2)#odbc driver pooling issue or could use a queue to limit connections
            thread.setDaemon(True)
            thread.start()
        for table in (sorted(tables)):
            queue.put(table)


    else:
        for table in (sorted(tables)):
            process_table_non_threaded(table)
    if args.threaded:

        queue.join()


