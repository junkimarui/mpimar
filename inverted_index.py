#!/usr/bin/env python
import sys
import codecs
import os
import json
from mpimar import MapReduceJob

class InvertedIndexJob(MapReduceJob):
    def __init__ (self,mapper_num,reducer_num,input_file,out_file):
        MapReduceJob.__init__(self,
                              {"name":"inverted_index",
                               "temp_dir":"/tmp",
                               "mapper":mapper_num,
                               "reducer":reducer_num,
                               "out_file":out_file,
                               "map_from_file":0})
        self.input_files = input_file.split(",")

    def distribute(self):
        for input_file in self.input_files:
            for line in codecs.open(input_file,'r',"utf_8"):
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
        self.emit((word,tf))

reload(sys)
sys.setdefaultencoding('utf-8')
argv = sys.argv
if len(argv) < 5:
    print "Usage: mpirun -np [number of process] python %s [mapper num] [reducer num] [file names] [out_file]" % argv[0]
    quit()
job = InvertedIndexJob(int(argv[1]),int(argv[2]),argv[3],argv[4]+".json")
job.start()

if job.isMaster():
    #convert from json
    fout = codecs.open(argv[4],"w","utf_8")
    for line in open(argv[4]+".json","r"):
        obj = json.loads(line.rstrip())
        tfs = []
        for fname in obj[1].keys():
            tfs.append(fname+" "+str(obj[1][fname]))
        fout.write(obj[0]+" "+" ".join(tfs)+"\n")
    #delete json file
    os.remove(argv[4]+".json")

