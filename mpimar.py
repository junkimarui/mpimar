#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
mpimar (MPI Map Reduce Library), pronounced as /empima:r/

"""
import boost.mpi as mpi
import os

class MapReduceJob(object):
    #args = {"name":"NAME", "temp_dir":"TEMP_DIR", "mapper":0.5, "reducer":0.5, "out_file":"OUTPUT FILE"}
    def __init__ (self,args):
        self.MAPINTAG = 1
        self.MAPOUTTAG = 2
        self.REDINTAG = 3
        self.REDOUTTAG = 4

        self.mapper_num = args["mapper"]
        self.reducer_num = args["reducer"]
        self.name = args["name"] if args.has_key("name") else "job"
        self.temp_dir = args["temp_dir"] if args.has_key("temp_dir") else "/tmp"
        self.out_file = args["out_file"] if args.has_key("out_file") else self.name+".txt"
        self.emit_idx = 0
        self.data_ = []

    #abstract methods
    def distribute(self): print('implement distribute method')
    def map(self,val): print('implement map method')
    def reduce(self,keyvals): print('implement reduce method')

    #utility methods
    def isMaster(self): return mpi.world.rank == 0
    def isMapper(self): return mpi.world.rank > 0 and mpi.world.rank <= self.mapper_num
    def getID(self): return self.name + "_" + str(mpi.world.rank)
    def emit(self,obj):
        if self.isMaster():
            self.emit_idx = (self.emit_idx + 1) % self.mapper_num
            mpi.world.send(self.emit_idx+1,self.MAPINTAG,obj)
        elif self.isMapper():
            self.data_.append(obj)
        else:
            self.data_.append(obj)

    def master(self):
        self.distribute()
        #tells mappers to finish their jobs
        for mapper in range(1,self.mapper_num+1):
            mpi.world.send(mapper,self.MAPINTAG," ")
        #receives file names from mapper
        files = {}
        for mapper in range(1,self.mapper_num+1):
            f = mpi.world.recv(mapper,self.MAPOUTTAG)
            for key in f.keys():
                if files.has_key(key):
                    files[key].append(f[key])
                else:
                    files[key] = [f[key]]
        #sends file names to reducers
        for key in files.keys():
            mpi.world.send(self.mapper_num + 1 + int(key), self.REDINTAG, files[key])
        #receives file names from reducers and concatinates them
        fout = open(self.out_file,"w")
        for reducer in range(self.mapper_num + 1, mpi.size):
            fname = mpi.world.recv(reducer,self.REDOUTTAG)
            for line in open(fname,"r"):
                fout.write(line)
            os.remove(fname)
        for key in files.keys():
            for fname in files[key]:
                os.remove(fname)

    def getHashKey(self,key): return hash(key) % self.reducer_num
        
    def mapper(self):
        msg = mpi.world.recv(0,self.MAPINTAG)
        while msg != " ":
            self.map(msg)
            msg = mpi.world.recv(0,self.MAPINTAG)
        
        files = {}
        #shuffle and sort
        for row in sorted(self.data_):
            key = row[0]
            rkey = str(self.getHashKey(key))
            fname = self.temp_dir+"/"+self.getID()+"mr_"+rkey+".txt"
            files[rkey] = fname
            f = open(fname,"a")
            f.write(key+" "+str(row[1])+"\n")
            f.close()
        mpi.world.send(0,self.MAPOUTTAG,files)
        
    def reducer(self):
        files = mpi.world.recv(0,self.REDINTAG)
        fobjs = []
        for file in files:
            fobjs.append(open(file,"r"))
        key_prev = ""
        vals = []
        lines = {}
        while True:
            #read next line with the smallest hash value
            for fobj in fobjs:
                if (not lines.has_key(fobj)) or (lines[fobj] == ""):
                    lines[fobj] = fobj.readline()
                    if lines[fobj]:
                        lines[fobj] = lines[fobj].rstrip()
                    else:
                        #delete if it's already read
                        fobjs.remove(fobj)
                        del(lines[fobj])

            if len(fobjs) == 0:
                break

            fmin = None
            kmin = ""
            vmin = ""
            for fobj in fobjs:
                keyval = lines[fobj].split(" ",2)
                if kmin == "" or keyval[0] < kmin:
                    kmin = keyval[0]
                    fmin = fobj
                    vmin = keyval[1]

            #gathers values according as keys
            if key_prev == kmin:
                vals.append(vmin)
            else:
                if len(vals) != 0:
                    self.reduce((key_prev,vals))
                key_prev = kmin
                vals = [vmin]
            lines[fmin] = "" #for next loop
            
        if len(vals) != 0:
            self.reduce((key_prev,vals))

        #output file
        fname = self.temp_dir+"/"+self.getID()+"_red.txt"
        f = open(fname,"w")
        for row in self.data_:
            f.write(row[0]+" "+str(row[1])+"\n")
        f.close()
        mpi.world.send(0,self.REDOUTTAG,fname)

    def start(self):
        if self.isMaster():
            self.master()
        elif self.isMapper():
            self.mapper()
        else:
            self.reducer()
            
