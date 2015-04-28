import pyodbc,datetime,csv,sys, xlsxwriter,argparse, os,pipes

def create_connection(databasename, servername='WIN-MK2SGQAU2T7'):
    try:
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+servername+';\
                      DATABASE='+databasename+';\
                       Trusted_Connection=Yes;\
                      CHARSET=UTF8')
        cursor = cnxn.cursor()
        return cursor
    except pyodbc.DatabaseError:
        print ("Connection Error! See below")
        raise
   

def find_tables(cursor):
    cursor.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    tables=cursor.fetchall()
    return tables

def create_dirs(database, type_dir):
    try:
        os.makedirs(database+"/"+type_dir)
       
    except OSError as e: 
        print (e)
        pass
    return database+"/"+type_dir+"/"

class Table:
    def __init__(self,  tablename):
        self.tablename=tablename
     
 
    
         
    def retrieve_data(self, cursor):
         self.rows_data = cursor.execute(u"SELECT * FROM ["+self.tablename+"]").fetchall()
    
    def __iter__(self):
        return self

 

    def retrieve_schema(self, cursor):
        self.colnames=cursor.execute(u"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS  where TABLE_NAME=? and\
                              TABLE_SCHEMA=?",self.tablename,'dbo').fetchall()

        
    def preprocess(self, paths):
        workbook = xlsxwriter.Workbook(paths[1]+self.tablename+".xlsx")
        worksheet = workbook.add_worksheet(self.tablename)
        proc_colnames = []
        for cid, colheader in enumerate(self.colnames):
            worksheet.write(0,cid, colheader[0])
            proc_colnames.append(colheader[0])
        self.write2csv(paths[0], proc_colnames)
        for rid, row in enumerate(self.rows_data):
            row_list=[]
            for cid, data in enumerate(row):
                
                 if isinstance(data, datetime.datetime):
                    row_list.append(data.isoformat(' '))
                    worksheet.write(rid+1, cid, data.isoformat(' '))
                 elif isinstance(data,bytearray):
                    seq = "".join([byte.decode('utf-8') for byte in data])
                    row_list.append(data)
                    worksheet.write(rid+1, cid, str(seq))
                 else:
                    row_list.append(data)
                    worksheet.write(rid+1, cid, data)
            self.write2csv(paths[0], row_list)
               
        workbook.close()
 
    def write2csv(self,path, row_list):
        with open(path+self.tablename+".csv", 'a+', newline='', encoding="utf-8") as csvfile:
                tablewriter = csv.writer(csvfile, delimiter=',')
                tablewriter.writerow(row_list)

    def write2xls(self,path, row,col ,data):
          workbook = xlsxwriter.Workbook(path+self.tablename+".xlsx","w+")
          worksheet = workbook.add_worksheet()
          worksheet.write(row, col, data)
          workbook.close()
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db", help="Specify database name")
    parser.add_argument("--server",help="Specify Server Name")
    args = parser.parse_args()
  
    if args.server:
        cursor = create_connection(args.db, args.server)
    else:
        cursor = create_connection(args.db)
 
    tables = find_tables(cursor)
    paths=[]
    paths.append(create_dirs(args.db, "csv"))
    paths.append(create_dirs(args.db, "xls"))
    for table in tables:
        t = Table(table[2])
        print ("Started parsing table {0}".format(t.tablename))
       
        t.retrieve_data(cursor)
        t.retrieve_schema(cursor)
        row_list = []

        
        t.preprocess(paths)
        print ("Finished parsing table {0}".format(t.tablename))
      
                                
  

