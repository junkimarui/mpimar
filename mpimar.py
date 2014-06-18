#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
mpimar (MPI Map Reduce Library), pronounced as /empima:r/

"""
import boost.mpi as mpi
import os
import os.path
import glob
import json

class MapReduceJob(object):
    #args = {"name":"NAME", "temp_dir":"TEMP_DIR", "mapper":0.5, "reducer":0.5, "out_file":"OUTPUT FILE"}
    def __init__ (self,args):
        self.MAPINTAG = 1
        self.MAPOUTTAG = 2
        self.REDINTAG = 3
        self.REDOUTTAG = 4
        self.MAPFILEREQTAG = 5
        self.MAPFILETAG = 6
        self.REDFILEREQTAG = 7
        self.REDFILETAG = 8
        self.FINISHTAG = 9

        self.mapper_num = args["mapper"]
        self.reducer_num = args["reducer"]
        self.name = args["name"] if args.has_key("name") else "job"
        self.temp_dir = args["temp_dir"] if args.has_key("temp_dir") else "/tmp"
        self.out_file = args["out_file"] if args.has_key("out_file") else self.name+".txt"
        self.emit_idx = 0 #for master
        self.data_ = [] #for mapper

    #abstract methods
    def distribute(self): print('implement distribute method')
    def map(self,val): print('implement map method')
    def reduce(self,keyvals): print('implement reduce method')

    #utility methods
    def isMaster(self): return mpi.world.rank == 0
    def isMapper(self): return mpi.world.rank in self.mappers()
    def isReducer(self): return mpi.world.rank in self.reducers()
    def mappers(self): return range(1,self.mapper_num+1)
    def reducers(self): return range(self.mapper_num + 1,mpi.size)
    def getID(self): return self.name + "_" + str(mpi.world.rank)
    def emit(self,obj):
        if self.isMaster():
            self.emit_idx = (self.emit_idx + 1) % self.mapper_num
            mpi.world.send(self.emit_idx+1,self.MAPINTAG,json.dumps(obj))
        elif self.isMapper():
            self.data_.append(obj)
        else:
            self.reducer_file.write(json.dumps(obj)+"\n")

    def master(self):
        self.distribute()
        #tells mappers to finish their jobs
        for mapper in self.mappers():
            mpi.world.send(mapper,self.MAPINTAG,"EOF")
        #receives file names from mapper
        files = {}
        for mapper in self.mappers():
            f = mpi.world.recv(mapper,self.MAPOUTTAG)
            for rkey in f.keys():
                if files.has_key(rkey):
                    files[rkey][str(mapper)] = f[rkey]
                else:
                    files[rkey] = {str(mapper):f[rkey]}                
        #sends file names to reducers
        for rkey in files.keys():
            mpi.world.send(self.mapper_num + 1 + int(rkey), self.REDINTAG, files[rkey])

        #receives file request from reducers
        for reducer in self.reducers():
            fnames = mpi.world.recv(reducer,self.MAPFILEREQTAG)
            for mkey in fnames.keys():
                mpi.world.send(int(mkey),self.MAPFILEREQTAG,(reducer,fnames[mkey]))

        #receives file names from reducers and concatinates them
        fout = open(self.out_file,"w")
        for reducer in self.reducers():
            fname = mpi.world.recv(reducer,self.REDOUTTAG)
            if os.path.exists(fname):
                for line in open(fname,"r"):
                    fout.write(line)
                try:
                    os.remove(fname)
                except:
                    print fname," might not be deleted(master)"
            else:
                mpi.world.send(reducer,self.REDFILEREQTAG,fname)
                content = mpi.world.recv(reducer,self.REDFILETAG)
                fout.write(content)
                content = ""

        #shut down mapper
        for mapper in self.mappers():
            mpi.world.send(mapper,self.MAPFILEREQTAG,"")
        #shut down reducer
        for reducer in self.reducers():
            mpi.world.send(reducer,self.REDFILEREQTAG,"")
        #wait for mappers and reducers
        msgs = []
        for child in self.mappers()+self.reducers():
            msgs.extend(mpi.world.recv(child,self.FINISHTAG))
        for msg in msgs:
            print msg

    def getHashKey(self,key): return hash(key) % self.reducer_num
        
    def mapper(self):
        #create directory if it doesn't exists
        if not os.path.exists(self.temp_dir):
            try:
                os.mkdir(self.temp_dir)
            except:
                if not os.path.exists(self.temp_dir):
                    print self.temp_dir, "couldn't be created (",self.getID(),")"
        #receive data from master
        fname_in = self.temp_dir+"/"+self.getID()+"map.txt"
        fin = open(fname_in,"w")
        msg = mpi.world.recv(0,self.MAPINTAG)
        while msg != "EOF":
            fin.write(msg+"\n")
            msg = mpi.world.recv(0,self.MAPINTAG)
        fin.close()
        #map step
        for line in open(fname_in,"r"):
            self.map(json.loads(line.rstrip()))
        #delete temporary files if they exist
        tmpfiles = glob.glob(self.temp_dir+"/"+self.getID()+"mr_*")
        for f in tmpfiles:
            os.remove(f)
        #shuffle and sort
        files = {}
        for row in sorted(self.data_):
            key = row[0]
            rkey = str(self.getHashKey(key))
            fname = self.temp_dir+"/"+self.getID()+"mr_"+rkey+".txt"
            files[rkey] = fname
            f = open(fname,"a")
            f.write(json.dumps(row)+"\n")
            f.close()
        self.data_ = [] #free memory
        mpi.world.send(0,self.MAPOUTTAG,files)
        msg = mpi.world.recv(0,self.MAPFILEREQTAG)
        while msg != "":
            (reducer,fname) = msg
            #not good implementation...
            content = open(fname,"r").read()
            mpi.world.send(reducer,self.MAPFILETAG,content)
            content = ""
            msg = mpi.world.recv(0,self.MAPFILEREQTAG)
        #delete temporary files
        msgs = []
        for fname in (files.values()+[fname_in]):
            if os.path.exists(fname):
                try:
                    os.remove(fname)
                except:
                    msgs.append(fname+" might not be deleted(map)")
        mpi.world.send(0,self.FINISHTAG,msgs)
        
    def reducer(self):
        #create directory if it doesn't exists
        if not os.path.exists(self.temp_dir):
            try:
                os.mkdir(self.temp_dir)
            except:
                if not os.path.exists(self.temp_dir):
                    print self.temp_dir, "couldn't be created (",self.getID(),")"
        #receive file list
        files = mpi.world.recv(0,self.REDINTAG)
        #copy file if it doesn't exist
        reqfiles = {}
        for mkey in files.keys():
            if not os.path.exists(files[mkey]):
                reqfiles[mkey] = files[mkey]
        mpi.world.send(0,self.MAPFILEREQTAG,reqfiles)
        for mkey in reqfiles.keys():
            f = open(reqfiles[mkey],"w")
            content = mpi.world.recv(int(mkey),self.MAPFILETAG)
            f.write(content)
            f.close()
        
        #output file
        fname_out = self.temp_dir+"/"+self.getID()+"_red.txt"
        self.reducer_file = open(fname_out,"w")
        #open files
        fobjs = []
        for mapper in files.keys():
            fobjs.append(open(files[mapper],"r"))
        key_prev = ""
        vals = []
        lines = {}
        while True:
            #read next line
            for fobj in fobjs:
                if (not lines.has_key(fobj)) or (lines[fobj] == ""):
                    lines[fobj] = fobj.readline()
                    if lines[fobj]:
                        lines[fobj] = json.loads(lines[fobj].rstrip())
                    else:
                        #delete if it's already read
                        fobjs.remove(fobj)
                        del(lines[fobj])
            #if finished reading all the files
            if len(fobjs) == 0:
                break
            #takes the smallest string from next lines
            fmin = None
            kmin = ""
            vmin = ""
            for fobj in fobjs:
                row = lines[fobj]
                if kmin == "" or row[0] < kmin:
                    kmin = row[0]
                    fmin = fobj
                    vmin = row[1]
            #gathers values according as keys
            if key_prev == kmin:
                vals.append(vmin)
            else:
                if len(vals) != 0:
                    self.reduce((key_prev,vals))
                key_prev = kmin
                vals = [vmin]
            lines[fmin] = "" #for next loop
        #finishing reduce step
        if len(vals) != 0:
            self.reduce((key_prev,vals))
        self.reducer_file.close()
        #send result to master node
        mpi.world.send(0,self.REDOUTTAG,fname_out)
        msg = mpi.world.recv(0,self.REDFILEREQTAG)
        while msg != "":
            content = open(fname_out,"r").read()
            mpi.world.send(0,self.REDFILETAG,content)
            msg = mpi.world.recv(0,self.REDFILEREQTAG)
        #delete files
        msgs = []
        for fname in files.values():
           if os.path.exists(fname):
               try:
                   os.remove(fname)
               except:
                   msgs.append(fname+" might not be deleted(reduce)")
        if os.path.exists(fname_out):
            try:
                os.remove(fname_out)
            except:
                msgs.append(fname_out+" might not be deleted(reduce)")
        mpi.world.send(0,self.FINISHTAG,msgs)

    def start(self):
        if self.isMaster():
            self.master()
        elif self.isMapper():
            self.mapper()
        else:
            self.reducer()
            
