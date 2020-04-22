#!/usr/bin/env python

import argparse, re, socket, collections, os, csv
from dataclasses import dataclass


parser = argparse.ArgumentParser()
parser.add_argument("user")
parser.add_argument("log")
parser.add_argument("csvfile")
args = parser.parse_args()

r = re.compile('^.*([A-Za-z]{3}\s\d{2}\s(\d{2}\:){2}\d{2}).*dovecot:\s([a-zA-Z0-9-]+):\s([a-zA-Z]+[a-zA-Z0-9\)\(\s,]*):\s.*rip=(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*$')


@dataclass
class Record:
    fname: str
    connection_time: str
    IP: str
    login_type: str
    secure: bool
    connection_status: str

    def __iter__(self):
        return (attr for attr in (self.fname, self.connection_time,
                    self.IP, self.login_type, self.secure, self.connection_status))
    

def constant_factory(val):
    return lambda: val
    


def read_from_log(fname):
  
    with open(fname) as input:
        for line in input:
            m = r.match(line)
          
            if args.user in line:
                if m:
                    if not "TLS" in line and not "secured" in line:
                        secure = False
                    else:
                        secure = True
               
                    if "Login" == m.group(4) or "Aborted" in m.group(4):
                        records.append(Record(os.path.basename(fname),
                                              m.group(1), m.group(5),m.group(3), secure, m.group(4)))
                        
                   
                        

def write_to_csv(csvfile):

    with open(csvfile, "a+") as csvfile:
     
        writer = csv.writer(csvfile, delimiter=',')
          
        [writer.writerow([*record]) for record in records]


def write_header(csvfile):
    
    with open(csvfile, "a+") as csvfile:
     
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(["FILE", "CONNECTION TIME", "IP ADDRESS","CONNECTION TYPE", "SECURE CONNECTION","CONNECTION STATUS"])
          



if __name__ == "__main__":
    if os.path.exists(args.csvfile):
            os.remove(args.csvfile)
    
    write_header(args.csvfile)
    records = []
    if os.path.isdir(args.log):
    
        for fname in os.listdir(args.log):
            if fname.startswith("round"):
                continue
           
            read_from_log(os.path.join(args.log, fname))
            write_to_csv(args.csvfile)
    else:

        read_from_log(args.log)
        write_to_csv(args.csvfile)
    


