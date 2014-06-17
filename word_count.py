#!/usr/bin/env python
import sys
import codecs
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
if len(argv) < 4:
    print "Usage: mpirun -np [number of process] python word_count.py [mapper num] [reducer num] [filename] [outfile]"
    quit()
if len(argv) > 4:
    job = WordCountJob(int(argv[1]),int(argv[2]),argv[3],argv[4])
else:
    job = WordCountJob(int(argv[1]),int(argv[2]),argv[3])
job.start()
