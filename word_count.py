#!/usr/bin/env python
import sys
import codecs
import os
import json
from mpimar import MapReduceJob

class WordCountJob(MapReduceJob):
    def __init__ (self,spout_num,mapper_num,reducer_num,input_file,out_file):
        MapReduceJob.__init__(self,
                              {"name":"wordcount",
                               "temp_dir":"/tmp",
                               "spout":spout_num,
                               "mapper":mapper_num,
                               "reducer":reducer_num,
                               "out_file":out_file})
        self.input_files = input_file.split(",")

    def distribute(self,spout_idx):
        for file_idx in range(0,len(self.input_files)):
            if file_idx % len(self.spouts()) != spout_idx: continue
            input_file = self.input_files[file_idx]
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
if len(argv) < 6:
    print "Usage: mpirun -np [number of process] python %s [spout num] [mapper num] [reducer num] [filename] [outfile]" % (argv[0])
    quit()
out_file = argv[5]
job = WordCountJob(int(argv[1]),int(argv[2]),int(argv[3]),argv[4],out_file+".json")
job.start()

if job.isMaster():
    #convert from json
    fout = codecs.open(out_file,"w","utf_8")
    for line in open(out_file+".json","r"):
        obj = json.loads(line.rstrip())
        fout.write(obj[0]+" "+str(obj[1])+"\n")
    #delete json file
    os.remove(out_file+".json")
