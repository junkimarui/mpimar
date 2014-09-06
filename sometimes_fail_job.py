#!/usr/bin/env python
import sys
import codecs
import os
import json
import random
from mpimar import MapReduceJob

class InvertedIndexJob(MapReduceJob):
    def __init__ (self,spout_num,mapper_num,reducer_num,input_file,out_file):
        MapReduceJob.__init__(self,
                              {"name":"inverted_index",
                               "temp_dir":"/tmp",
                               "mapper":mapper_num,
                               "reducer":reducer_num,
                               "out_file":out_file,
                               "spout":spout_num,
                               "allow_error_num":15})
        self.input_files = input_file.split(",")

    def distribute(self,spout_idx):
        for file_idx in range(0,len(self.input_files)):
            if file_idx % len(self.spouts()) != spout_idx: continue
            input_file = self.input_files[file_idx]
            for line in codecs.open(input_file,'r',"utf_8"):
                line = line.rstrip()
                self.emit((input_file,line))
    
    def map(self,tpl):
        file_name = tpl[0]
        words = tpl[1].split(' ')
        num = random.random()
        if num < 0.001:
            raise Exception("random error "+str(num))
        for word in words:
            self.emit((word,file_name))

    def reduce(self,keyvals):
        word = keyvals[0]
        filenames = keyvals[1]
        tf = {}
        num = random.random()
        if num < 0.001:
            raise Exception("random error "+str(num))
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
    print "Usage: mpirun -np [number of process] python %s [spout num] [mapper num] [reducer num] [file names] [out_file]" % argv[0]
    quit()
out_file = argv[5]
job = InvertedIndexJob(int(argv[1]),int(argv[2]),int(argv[3]),argv[4],out_file+".json")
job.start()

if job.isMaster():
    #convert from json
    fout = codecs.open(out_file,"w","utf_8")
    for line in open(out_file+".json","r"):
        obj = json.loads(line.rstrip())
        tfs = []
        for fname in obj[1].keys():
            tfs.append(fname+" "+str(obj[1][fname]))
        fout.write(obj[0]+" "+" ".join(tfs)+"\n")
    #delete json file
    os.remove(out_file+".json")

