#!/usr/bin/python
# coding: utf-8
# -*- coding: utf-8 -*-
"""Hash matching of ed2k hashes"""

from xlrd3 import open_workbook
from xlwt3 import Workbook
import hashlib
from binascii import hexlify
from functools import reduce
from os import listdir,path,walk
from os.path import isfile,abspath,isdir,join,getsize
#f = open("matches.txt",'w')
def cmpHashes(outputfile,hashesDict,metfile):
    """compare hashes of met xls file and file ed2key hashes"""
    wb = open_workbook(metfile)
    
    s=wb.sheets()[0]
    sheet = wb.sheet_by_index(0)         #input sheet
    
    book = Workbook()
    
    sheet1=book.add_sheet('Found')
    sheet2=book.add_sheet("NOT FOUND")
    
    #print ("number of roews",s.nrows)
    hashCalcprog=[]
    filesize=[]
    knowndotMedHashes=[]
    lastpostedUTC=[]
    lastsharedUTC=[]
    requestsAccepted=[]
    bytesUploaded=[]
    
    for i in range(sheet.nrows):#read hashes of met xls file
        #also includes labels
        knowndotMedHashes.append(sheet.cell_value(i,17).lower())
        filesize.append(sheet.cell_value(i,2))
        lastpostedUTC.append(sheet.cell_value(i,5))
        lastsharedUTC.append(sheet.cell_value(i,6)) 
        requestsAccepted.append(sheet.cell_value(i,8))
        bytesUploaded.append(sheet.cell_value(i,9))
    hash_indexes={}
    #hash_indexes=[knowndotMed.index(hash)
    i=0
    k=0
   # print (knowndotMedHashes)
    sheet1.write(k,0,'filename' )
    sheet1.write(k,2,filesize[k])
    sheet1.write(k,3,lastpostedUTC[k])
    sheet1.write(k,4,lastsharedUTC[k])
    sheet1.write(k,5,requestsAccepted[k])
    sheet1.write(k,1,'hash')
    
    for hash in  hashesDict.keys():
       
      
        try:
            
            index=knowndotMedHashes.index(hash)#retrieve index of matched hash
            hash_indexes[index]=hashesDict[hash]
            sheet1.write(k+1,0,hashesDict[hash] )
            sheet1.write(k+1,2,filesize[index])
            sheet1.write(k+1,3,lastpostedUTC[index])
            sheet1.write(k+1,4,lastsharedUTC[index])
            sheet1.write(k+1,5,requestsAccepted[index])
            sheet1.write(k+1,1,hash)
            k+=1
        except ValueError:
                  
            notFoundfile=hashesDict[hash]
            sheet2.write(i+1,0,hashesDict[hash] )
            sheet2.write(i+1,1,hash)
   

            i+=1
            continue
    
    
    i=0
    
    book.save(outputfile)
#print (hash_indexes)
#f.write(str(hash_indexes))
#f.close()



def hash_file(file_path):
    """ Returns the ed2k hash of a given file. """
    sys.stdout.write("Finding hash for %s ...\n" %file_path.encode("utf8"))
    md4 = hashlib.new('md4').copy
 
    def gen(f):
        while True:
            x = f.read(9728000)
            if x: yield x
            else: return
 
    def md4_hash(data):
        m = md4()
        m.update(data)
        return m
 
    with open(file_path, 'rb') as f:
        a = gen(f)
        hashes = [md4_hash(data).digest() for data in a]# Building up a list of md4 hashes associated with 9500KB blocks
      #  print(type(hashes[0]),type(a))
        if len(hashes) == 1:
            #print (type(hashes[0]),hashes[0])
            hash=hexlify(hashes[0])
            return str(hash)[1:].replace('\'','')  if str(hash)[0]=='b' else str(hash).replace('\'','') 
        else: return str(md4_hash(reduce(lambda a,d: a + d, hashes)).hexdigest())#function, items [, initial])

def genfile(inputfolder):
  
  
    for dirpath,dirnames,filenames in  walk(inputfolder):   
          #sys.stdout.write("Retrieving hashes in %d ...\n" %len(dirnames) )
          for file in filenames:
         
            yield file,dirpath
    
        
if __name__ == '__main__':
    import sys
    if len(sys.argv) == 4:
       
       
        inputfolder = abspath(sys.argv[1])
        metfile=abspath(sys.argv[2])
        outputfile= abspath(sys.argv[3])
      #  print("i",inputfolder, outputfile)
        files=[]
        hashes=[]
       # hashes=list(map(lambda file:hash_file(join(inputfolder, file)),filter(lambda file:isfile(join(inputfolder,file)),listdir(inputfolder))))
        for file,dirpath in genfile(inputfolder):   
       #   sys.stdout.write("Retrieving hashes in %s ...\n" %dirpath)
          if getsize(dirpath+"\\"+file)!=0:
            files.append(dirpath+"\\"+file)
            hashes.append(hash_file(join(dirpath, file)))
       # files=list(filter(lambda file: isfile(join(inputfolder, file)),listdir(inputfolder)))
            sys.stdout.write("Retrieving hashes in %s ...\n" %inputfolder)
 
            hashesDict=dict(zip(hashes, files))
            sys.stdout.write("Comparing hashes ...\n")
            cmpHashes(outputfile,hashesDict,metfile)
          else:
            sys.stdout.write("Zero size file %s ...\n" %file)
              
    else:
        sys.stderr.write("Usage : python %s inputfolder metfile outputfile\n" % sys.argv[0])
        raise SystemExit(1)
    

        
           
