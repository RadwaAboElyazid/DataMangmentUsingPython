import argparse
import os
from subprocess import PIPE, Popen
from os import listdir
from os.path import isfile, join
import json
import pandas as pd
import csv
import re
import datetime



parser = argparse.ArgumentParser()
parser.add_argument("path",
                    help="Enter the dir path which include the files")

parser.add_argument("-u", action="store_true", dest="UNIX", default=False,
                    help="It keeps the UNIX format to the time")

args = parser.parse_args()
path = args.path


files = [item for item in listdir(args.path) if isfile(join(args.path, item))]
checksums = {}
duplicates = []
newfile = []
print(os.path.dirname(os.path.realpath(__file__)))

for filename in files:
    filepath = path + '/'+ filename
    with Popen(["md5sum", filepath], stdout=PIPE) as proc:
        checksum = proc.stdout.read().split()[0]
        if checksum in checksums:
            duplicates.append(filename)
        checksums[checksum] = filename

print(f"Found Duplicates: {duplicates}")

for f in duplicates:
     os.remove(path+'/'+f)    
     files.remove(f)


for filename in files:
     df = pd.DataFrame(columns = ['web_browser','operating_system','from_url','to_url','city','longitude','latitude','time_zone','time_in','time_out'])
     contents = open(path+'/'+filename).read()
     data = [json.loads(str(item)) for item in contents.strip().split('\n')]
     t = None
     h = None 
     a = None
     r = None
     webbrowser = None
     newfromURL= None
     newtoURL = None
     longitude = None
     latitude = None 
    
     for item in data:
          try:
             a = item.get('a')
             webbrowser = a.split(' (')[0]
             operatingsystem = ['windows','ubuntu','macintosh']
             os = 'unknown' 
             for s in operatingsystem:
                 if s in a.lower():
                     os = s
          except:
              pass
                
          try:             
             fromurl = item['r']
             fromurl = fromurl[7:]
             newfromURL = fromurl.split('/')[0]         
             tourl = item['u']
             tourl = tourl[7:]
             newtoURL = tourl.split('/')[0]
          except:
              pass
                  
          try:         
             longitude = item['ll'][0]

             latitude = item['ll'][1]
          except:
              pass   
              
          try:    
             if args.UNIX: 
                 t = item['t']
                 h = item['hc']
             else:         
                 ti = item['t']
                 timestamp = datetime.datetime.fromtimestamp(ti)
                 t = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                 to = item['hc']
                 timestamp = datetime.datetime.fromtimestamp(to)
                 h = timestamp.strftime('%Y-%m-%d %H:%M:%S')
          except:
               pass      
               
          mydic = { 'web_browser': webbrowser,
                    'operating_system': os,
                    'from_url': newfromURL,
                    'to_url': newtoURL,
                    'city': item.get('cy'),
                    'longitude': longitude,
                    'latitude': latitude,
                    'time_zone':item.get('tz'),
                    'time_in': t,
                    'time_out': h
                    
                   }
          
          df = df.append(mydic, ignore_index=True).fillna(0)

          
         
     df.to_csv(filename[:-5]+'.csv' , index=False) 

for f in files:
    print (path +'/'+ f)   
          
import time
start_time = time.time()
print("--- %s seconds ---" % (time.time() - start_time))        

     
     


              



  


   
