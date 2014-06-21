#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
mpimar (MPI Map Reduce Library), pronounced as /empima:r/
https://github.com/junkimarui/mpimar
"""
import boost.mpi as mpi
import os
import os.path
import glob
import random
import sys
try:
    import ujson as json
except ImportError:
    import json

class MapReduceJob(object):
    __author__ = "Junki Marui"
    __copyright__ = "Copyright (C) 2014 Junki Marui"
    __license__ = "MIT License"
    __version__ = "0.1"
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
        self.spout_num = args["spout"] if args.has_key("spout") else 1
        self.name = args["name"] if args.has_key("name") else "job"
        self.temp_dir = args["temp_dir"] if args.has_key("temp_dir") else "/tmp"
        self.out_file = args["out_file"] if args.has_key("out_file") else self.name+".txt"
        self.allow_error_num = args["allow_error_num"] if args.has_key("allow_error_num") else 0
        self.map_from_file = args["map_from_file"] if args.has_key("map_from_file") else 0
        self.debug_ = args["debug"] if args.has_key("debug") else 0
        self.error_ = []
        self.emit_idx = 0 #for master
        self.emit_reqs = mpi.RequestList()
        self.spout_array = range(0,self.spout_num)
        childarray = range(self.spout_num,mpi.size)
        if args.has_key("node_shuffle") and args["node_shuffle"] == 1:
            random.seed(1341) #to generate the same random variables
            random.shuffle(childarray)
        self.mapper_array = childarray[:self.mapper_num]
        self.reducer_array = childarray[self.mapper_num:]

    #abstract methods
    def distribute(self,spout_idx): print('implement distribute method')
    def map(self,val): print('implement map method')
    def reduce(self,keyvals): print('implement reduce method')

    #utility methods
    def isMaster(self): return mpi.world.rank == 0
    def isSpout(self): return mpi.world.rank in self.spouts()
    def isMapper(self): return mpi.world.rank in self.mappers()
    def isReducer(self): return mpi.world.rank in self.reducers()
    def spouts(self): return self.spout_array
    def mappers(self): return self.mapper_array
    def reducers(self): return self.reducer_array
    def getID(self): return self.name + "_" + str(mpi.world.rank)
    def getReducerFromKey(self,key): return self.reducers()[hash(key) % self.reducer_num]
    def emit(self,obj):
        if self.isSpout():
            self.emit_reqs.append(mpi.world.isend(self.mappers()[self.emit_idx],self.MAPINTAG,json.dumps(obj)))
            self.emit_idx = (self.emit_idx + 1) % self.mapper_num
            if self.emit_idx == 0:
                mpi.wait_all(self.emit_reqs)
                self.emit_reqs = mpi.RequestList()
        elif self.isMapper():
            self.shuffled_files[str(self.getReducerFromKey(obj[0]))].write(json.dumps(obj)+"\n")
        else:
            self.reducer_file.write(json.dumps(obj)+"\n")
    def setError(self,message):
        role = "reducer"
        if self.isMaster():
            role = "master"
        elif self.isMapper():
            role = "mapper"
        self.error_.append("[%d:%s] %s" % (mpi.world.rank,role,message))
    def checkErrorCount(self):
        if self.allow_error_num < len(self.error_):
            raise Exception("\n".join(self.error_)+"\n"+
                            "Too many errors occured in "+self.getID()+"\n")
    def spout(self):
        self.distribute(mpi.world.rank)
        if len(self.emit_reqs) > 0: mpi.wait_all(self.emit_reqs)
        #tells mappers to finish receiving data
        for mapper in self.mappers():
            mpi.world.send(mapper,self.MAPINTAG,"EOF")
        if self.debug_ == 1: sys.stderr.write("data sent from spout:"+str(mpi.world.rank)+"\n")

    def master(self):
        self.distribute(mpi.world.rank)
        if len(self.emit_reqs) > 0: mpi.wait_all(self.emit_reqs)
        #tells mappers to finish receiving data
        for mapper in self.mappers():
            mpi.world.send(mapper,self.MAPINTAG,"EOF")
        if self.debug_ == 1: sys.stderr.write("data sent from master\n")
        #receives file names from mappers
        files = {}
        for mapper in self.mappers():
            f = mpi.world.recv(mapper,self.MAPOUTTAG)
            for rkey in f.keys():
                if files.has_key(rkey):
                    files[rkey][str(mapper)] = f[rkey]
                else:
                    files[rkey] = {str(mapper):f[rkey]}
        if self.debug_ == 1: sys.stderr.write("master -> reducer (file list)\n")
        #sends file names to reducers
        for rkey in files.keys():
            mpi.world.send(int(rkey), self.REDINTAG, files[rkey])
        if self.debug_ == 1: sys.stderr.write("reducer -> master -> mapper(file request)\n")
        #receives file request from reducers
        for reducer in self.reducers():
            fnames = mpi.world.recv(reducer,self.MAPFILEREQTAG)
            for mkey in fnames.keys():
                mpi.world.send(int(mkey),self.MAPFILEREQTAG,(reducer,fnames[mkey]))
        if self.debug_ == 1: sys.stderr.write("reduce step\n")
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
                    self.setError(fname+" might not be deleted")
            else:
                mpi.world.send(reducer,self.REDFILEREQTAG,fname)
                content = mpi.world.recv(reducer,self.REDFILETAG)
                fout.write(content)
                content = ""
        if self.debug_ == 1: sys.stderr.write("shutting down\n")
        #shuts down mapper
        for mapper in self.mappers():
            mpi.world.send(mapper,self.MAPFILEREQTAG,"")
        #shuts down reducer
        for reducer in self.reducers():
            mpi.world.send(reducer,self.REDFILEREQTAG,"")
        #gathers error message from mappers and reducers
        for child in self.mappers()+self.reducers():
            self.error_.extend(mpi.world.recv(child,self.FINISHTAG))
        for error in self.error_:
            print error

    def mapper(self):
        #creates directory if it doesn't exists
        if not os.path.exists(self.temp_dir):
            try:
                os.mkdir(self.temp_dir)
            except:
                if not os.path.exists(self.temp_dir):
                    self.setError(self.temp_dir+" couldn't be created")
        #open shuffled files for writing
        self.shuffled_files = {}
        filenames = {}
        for reducer in self.reducers():
            rkey = str(reducer)
            filenames[rkey] = self.temp_dir+"/"+self.getID()+"mr_"+rkey+".txt"
            self.shuffled_files[rkey] = open(filenames[rkey],"w")
        
        #receives data from master
        fname_in = self.temp_dir+"/"+self.getID()+"map.txt"
        valid_spouts = self.spouts()[:]
        spout_idx = 0
        if self.map_from_file == 1:
            # received data stored in a file
            fin = open(fname_in,"w")
            while True:
                source = valid_spouts[spout_idx]
                msg = mpi.world.recv(source,self.MAPINTAG)
                if msg != "EOF":
                    fin.write(msg+"\n")
                else:
                    valid_spouts.remove(source)
                    if len(valid_spouts) == 0: break
                spout_idx = (spout_idx + 1) % len(valid_spouts)
            fin.close()
            line_num = 0
            for line in open(fname_in,"r"):
                try:
                    self.map(json.loads(line.rstrip()))
                except Exception as inst:
                    self.setError(inst.args[0]+" at line "+ str(line_num))
                    self.checkErrorCount()
                    line_num += 1
        else:
            #received data are not stored in a file
            while True:
                source = valid_spouts[spout_idx]
                msg = mpi.world.recv(source,self.MAPINTAG)
                if msg != "EOF":
                    try:
                        self.map(json.loads(msg))
                    except Exception as inst:
                        self.setError(inst.args[0])
                        self.checkErrorCount()
                else:
                    valid_spouts.remove(source)
                    if len(valid_spouts) == 0: break
                spout_idx = (spout_idx + 1) % len(valid_spouts)
        #finishes writing files
        for fobj in self.shuffled_files.values():
            fobj.close()
        #sorts shuffled files
        for fname in filenames.values():
            data = []
            for line in open(fname,"r"):
                data.append(json.loads(line.rstrip()))
            f = open(fname,"w")
            for row in sorted(data):
                f.write(json.dumps(row)+"\n")
            f.close()
        #send file list to master node
        mpi.world.send(0,self.MAPOUTTAG,filenames)
        #if a file is requested, send it to a reducer
        msg = mpi.world.recv(0,self.MAPFILEREQTAG)
        while msg != "":
            (reducer,fname) = msg
            content = open(fname,"r").read()
            mpi.world.send(reducer,self.MAPFILETAG,content)
            content = ""
            msg = mpi.world.recv(0,self.MAPFILEREQTAG)
        #deletes temporary files
        for fname in (filenames.values()+[fname_in]):
            if os.path.exists(fname):
                try:
                    os.remove(fname)
                except:
                    if os.path.exists(fname): self.setError(fname+" might not be deleted")
        mpi.world.send(0,self.FINISHTAG,self.error_)
        
    def reducer(self):
        #creates directory if it doesn't exists
        if not os.path.exists(self.temp_dir):
            try:
                os.mkdir(self.temp_dir)
            except:
                if not os.path.exists(self.temp_dir):
                    self.setError(self.temp_dir+ " couldn't be created")
        #receives file list
        files = mpi.world.recv(0,self.REDINTAG)
        #copies file if it doesn't exist
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
        
        #outputs file
        fname_out = self.temp_dir+"/"+self.getID()+"_red.txt"
        self.reducer_file = open(fname_out,"w")
        #opens files
        fobjs = []
        for mapper in files.keys():
            fobjs.append(open(files[mapper],"r"))
        key_prev = ""
        vals = []
        lines = {}
        while True:
            #reads next line
            for fobj in fobjs:
                if (not lines.has_key(fobj)) or (lines[fobj] == ""):
                    lines[fobj] = fobj.readline()
                    if lines[fobj]:
                        try:
                            nextline = lines[fobj].rstrip()
                            lines[fobj] = json.loads(nextline)
                        except Exception as inst:
                            lines[fobj] = ""
                            self.setError(inst.args[0]+" during reading: "+nextline)
                            self.checkErrorCount()
                    else:
                        #deletes if it's already read
                        del(lines[fobj])
            #if it finished reading all the files
            if len(lines.keys()) == 0:
                break
            #takes the smallest string from next lines
            fmin = None
            kmin = ""
            vmin = ""
            for fobj in fobjs:
                if not lines.has_key(fobj) or lines[fobj] == "": continue
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
                    try:
                        self.reduce((key_prev,vals))
                    except Exception as inst:
                        self.setError(inst.args[0]+" during processing key = %s" % (key_prev))
                        self.checkErrorCount()
                key_prev = kmin
                vals = [vmin]
            lines[fmin] = "" #for next loop
        #finishing reduce step
        if len(vals) != 0:
            try:
                self.reduce((key_prev,vals))
            except Exception as inst:
                self.setError(inst.args[0]+" during processing key = %s" % (key_prev))
                self.checkErrorCount()
        self.reducer_file.close()
        #sends result to master node
        mpi.world.send(0,self.REDOUTTAG,fname_out)
        msg = mpi.world.recv(0,self.REDFILEREQTAG)
        while msg != "":
            content = open(fname_out,"r").read()
            mpi.world.send(0,self.REDFILETAG,content)
            msg = mpi.world.recv(0,self.REDFILEREQTAG)
        #deletes files
        for fname in files.values()+[fname_out]:
           if os.path.exists(fname):
               try:
                   os.remove(fname)
               except:
                    if os.path.exists(fname):self.setError(fname+" might not be deleted")
        mpi.world.send(0,self.FINISHTAG,self.error_)

    def start(self):
        if self.isMaster():
            self.master()
        elif self.isSpout():
            self.spout()
        elif self.isMapper():
            self.mapper()
        else:
            self.reducer()
            
