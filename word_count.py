#!/usr/bin/env python
import sys
import codecs
import os
import json
from mpimar import MapReduceJob

class WordCountJob(MapReduceJob):
    def __init__ (self,mapper_num,reducer_num,input_file,out_file):
        MapReduceJob.__init__(self,{"name":"wordcount","temp_dir":"/tmp","mapper":mapper_num,"reducer":reducer_num,"out_file":out_file})
        self.input_files = input_file.split(",")

    def distribute(self):
        for input_file in self.input_files:
            for line in codecs.open(input_file,'r','utf_8'):
                line = line.rstrip()
                self.emit(line)

    def map(self,line):
        words = line.split(' ')
        for word in words:
            self.emit((word,1))

    def reduce(self,keyvals):
        key = keyvals[0]
        vals = keyvals[1]
        sum = 0
        for val in vals:
            sum += int(val)
        self.emit((key,sum))

reload(sys)
sys.setdefaultencoding('utf-8')        
argv = sys.argv
if len(argv) < 5:
    print "Usage: mpirun -np [number of process] python %s [mapper num] [reducer num] [filename] [outfile]" % (argv[0])
    quit()
job = WordCountJob(int(argv[1]),int(argv[2]),argv[3],argv[4]+".json")
job.start()

if job.isMaster():
    #convert from json
    fout = codecs.open(argv[4],"w","utf_8")
    for line in open(argv[4]+".json","r"):
        obj = json.loads(line.rstrip())
        fout.write(obj[0]+" "+str(obj[1])+"\n")
    #delete json file
    os.remove(argv[4]+".json")
