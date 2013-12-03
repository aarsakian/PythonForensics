#!/usr/bin/python                                                                
# coding: utf-8
# -*- coding: utf-8 -*-
"""Hash matching of ed2k hashes 
Author Armen Arsakian DEE LAB"""


from xlrd3 import open_workbook
from xlwt3 import Workbook,XFStyle, Borders, Pattern, Font
from os import listdir,path,walk
from os.path import isfile,abspath,isdir,join,getsize 
import re
  
def processFIle(metfile,keywordfile,outputfile):      
    """ searches for keywords in filenames that are suspicious regarding pedo content"""
    regexp = re.compile("!|[]|]|;|\?|\(|\)|-|_|\.|\,")

    wb = open_workbook(metfile)
    
    book = Workbook()
    
    sheet1=book.add_sheet('Found')
    sheet2=book.add_sheet("NOT FOUND")
    
    s=wb.sheets()[0]
    sheet = wb.sheet_by_index(0)         #input sheet
    
    book = Workbook()
    
    sheet1=book.add_sheet('Found')
    sheet2=book.add_sheet("NOT FOUND",cell_overwrite_ok=True)
    
    
    
    #print ("number of roews",s.nrows)
    hashCalcprog=[]
    filesize=[]
    knowndotMedHashes=[]
    lastpostedUTC=[]
    lastsharedUTC=[]
    requestsAccepted=[]
    bytesUploaded=[]
    filenames=[]
    keywords=[]
    
    with open(keywordfile) as f: # Returns a file object
      keywords=[line.replace('\n','').encode('utf8') for line in f] # Invokes readline() method on file
  
        
    k=0
    sheet1.write(k,0,'Α/Α')
    sheet1.write(k,1,'keyword')   
   
    for col   in range(sheet.ncols):
      sheet1.write(k,col+2,sheet.cell_value(0,col) )
  

    for i in range(sheet.nrows):#read hashes of met xls file
        if i>1:
          Found=False
          knowndotMedHashes.append(sheet.cell_value(i,17))
          filename=str(sheet.cell_value(i,0)).encode('utf8')
          filenames.append(filename)
          filesize=sheet.cell_value(i,2)
          lastpostedUTC=sheet.cell_value(i,5)
          lastsharedUTC=sheet.cell_value(i,6) 
          requestsAccepted=sheet.cell_value(i,8)
          bytesUploaded=sheet.cell_value(i,9)
        
          for term in regexp.sub(' ',filename.decode()).split(' '):
             
             if term.encode('utf8').lower() in keywords:
                print ("FOUND",term.encode('utf8').lower(),term.encode('utf8').lower() in keywords,type(term.encode('utf8').lower()))
                k+=1
                sheet1.write(k,0,k)
                sheet1.write(k,1,term)
                for col   in range(sheet.ncols):
               
                  sheet1.write(k,col+2,sheet.cell_value(i,col) )
              
              
                Found=True 
                break
          if not Found:
             sheet2.write(i,0,i)
             for col   in range(sheet.ncols):
               
                  sheet2.write(i,col+1,sheet.cell_value(i,col) )
              
            
     
    book.save(outputfile)
     
     
if __name__ == '__main__':
    import sys
    if len(sys.argv) == 4:
       
        try:
          keywordFile = abspath(sys.argv[1])
          metfile=abspath(sys.argv[2])
          outputfile= abspath(sys.argv[3])
      

        
     
     #   sys.stdout.write("Calculating hashes in %s ...\n" %filepath.encode("utf8"))
           
          processFIle(metfile,keywordFile,outputfile)      
        except IOError as e:
          sys.stderr.write("Error! A File is opened. Please Close File %s\n"%str(e).split('\\')[-1])
          raise SystemExit(1)  

         
              
    else:                                                                   
        sys.stderr.write("Usage : python %s keywordFile metFile outputFile\n **** Important Note ****\n The keyword file must be encoded in utf-8 as well as each line must contain only one keyword\n" % sys.argv[0])
        raise SystemExit(1)
    
