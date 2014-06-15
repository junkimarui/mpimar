#!/usr/bin/env python
import sys
from mpimar import MapReduceJob

class InvertedIndexJob(MapReduceJob):
    def __init__ (self,mapper_num,reducer_num,input_file,out_file):
        MapReduceJob.__init__(self,{"name":"inverted_index","temp_dir":"/home/marui/python/tmp","mapper":mapper_num,"reducer":reducer_num,"out_file":out_file})
        self.input_files = input_file.split(",")
        self.tmp = {}

    def distribute(self):
        for input_file in self.input_files:
            for line in open(input_file,'r'):
                line = line.rstrip()
                self.emit((input_file,line))
        
    def map(self,tpl):
        file_name = tpl[0]
        words = tpl[1].split(' ')
        for word in words:
            self.emit((word,file_name))

    def reduce(self,keyvals):
        word = keyvals[0]
        filenames = keyvals[1]
        tf = {}
        for filename in filenames:
            if tf.has_key(filename):
                tf[filename] += 1
            else:
                tf[filename] = 1
        vals = []
        for filename in tf.keys():
            vals.append(filename+" "+str(tf[filename]))
        self.emit((word," ".join(vals)))

argv = sys.argv
if len(argv) < 4:
    print "Usage: mpirun -np [number of process] python %s [mapper num] [reducer num] [file names] [out_file]" % argv[0]
    quit()
if len(argv) > 4:
    job = InvertedIndexJob(int(argv[1]),int(argv[2]),argv[3],argv[4])
else:
    job = InvertedIndexJob(int(argv[1]),int(argv[2]),argv[3])
job.start()
