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
import time
try:
    import ujson as json
except ImportError:
    import json
from operator import itemgetter
import hashlib

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
        self.MAPREQTAG = 10

        self.mapper_num = args["mapper"]
        self.reducer_num = args["reducer"]
        self.spout_num = args["spout"] if args.has_key("spout") else 1
        self.name = args["name"] if args.has_key("name") else "job"
        self.temp_dir = args["temp_dir"] if args.has_key("temp_dir") else "/tmp"
        self.out_file = args["out_file"] if args.has_key("out_file") else self.name+".txt"
        self.allow_error_num = args["allow_error_num"] if args.has_key("allow_error_num") else 0
        self.map_from_file = args["map_from_file"] if args.has_key("map_from_file") else 0
        self.spout_emit_num = args["spout_emit_num"] if args.has_key("spout_emit_num") else 10000
        self.spout_buffer = []
        self.shuffled_line_max = args["shuffled_line_max"] if args.has_key("shuffled_line_max") else 1000000
        self.reduced_line_max = args["reduced_line_max"] if args.has_key("reduced_line_max") else 100000
        self.debug_ = args["debug"] if args.has_key("debug") else 0
        self.error_ = []
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
    def getReducerFromKey(self,key):
        hash_val = int(hashlib.md5(key).hexdigest()[:16],16)
        return self.reducers()[hash_val % self.reducer_num]
    def emit(self,obj):
        if self.isSpout():
            self.spout_buffer.append(json.dumps(obj))
            if len(self.spout_buffer) >= self.spout_emit_num:
                self.sendFromSpout(self.MAPINTAG,self.spout_buffer)
                self.spout_buffer = []
        elif self.isMapper():
            rkey = str(self.getReducerFromKey(obj[0]))
            if self.shuffled_lines[rkey] % self.shuffled_line_max == 0:
                fname = self.temp_dir+"/"+self.getID()+"mr_"+rkey+"_"+str(self.shuffled_lines[rkey] // self.shuffled_line_max)+".txt"
                self.shuffled_filenames[rkey].append(fname)
                if self.shuffled_fobj.has_key(rkey):
                    self.shuffled_fobj[rkey].close()
                self.shuffled_fobj[rkey] = open(fname,"w")
            self.shuffled_fobj[rkey].write(json.dumps(obj)+"\n")
            self.shuffled_lines[rkey] += 1
        else:
            self.reducer_file.write(json.dumps(obj)+"\n")

    def sendFromSpout(self,tag,obj_json):
        status = mpi.world.iprobe(tag=self.MAPREQTAG)
        while status is None:
            time.sleep(0.01)
            status = mpi.world.iprobe(tag=self.MAPREQTAG)
        done_msg = mpi.world.recv(status.source,self.MAPREQTAG)
        mpi.world.send(status.source,tag,obj_json)

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
        self.sendFromSpout(self.MAPINTAG,self.spout_buffer) #send the rest
        #tells mappers to finish receiving data
        reqs = mpi.RequestList()
        for mapper in self.mappers():
            done_msg = mpi.world.recv(mapper,self.MAPREQTAG)
            reqs.append(mpi.world.isend(mapper,self.MAPINTAG,"EOF"))
        mpi.wait_all(reqs)
        if self.debug_ == 1: sys.stderr.write("data sent from spout:"+str(mpi.world.rank)+"\n")

    def master(self):
        self.distribute(mpi.world.rank)
        self.sendFromSpout(self.MAPINTAG,self.spout_buffer) #send the rest
        #tells mappers to finish receiving data
        reqs = mpi.RequestList()
        for mapper in self.mappers():
            done_msg = mpi.world.recv(mapper,self.MAPREQTAG)
            reqs.append(mpi.world.isend(mapper,self.MAPINTAG,"EOF"))
        mpi.wait_all(reqs)
        if self.debug_ == 1: sys.stderr.write("data sent from master\n")
        #receives file names from mappers
        files = {}
        mapnum_rest = len(self.mappers())
        while mapnum_rest != 0:
            status = mpi.world.iprobe(tag=self.MAPOUTTAG)
            while status is None:
                time.sleep(0.1)
                status = mpi.world.iprobe(tag=self.MAPOUTTAG)
            mapper = status.source
            mapnum_rest -= 1
            if self.debug_ == 1: sys.stderr.write("mapper %d -> master (shuffled file)\n" % mapper)
            f = mpi.world.recv(mapper,self.MAPOUTTAG)
            for rkey in f.keys():
                if files.has_key(rkey):
                    files[rkey][str(mapper)] = f[rkey]
                else:
                    files[rkey] = {str(mapper):f[rkey]}
        #sends file names to reducers
        if self.debug_ == 1: sys.stderr.write("master -> reducer (file list)\n")
        for rkey in files.keys():
            mpi.world.send(int(rkey), self.REDINTAG, files[rkey])
        #receives file request from reducers
        if self.debug_ == 1: sys.stderr.write("reducer -> master -> mapper(file request)\n")
        for reducer in self.reducers():
            fnames = mpi.world.recv(reducer,self.MAPFILEREQTAG)
            for mkey in fnames.keys():
                for fname in fnames[mkey]:
                    mpi.world.send(int(mkey),self.MAPFILEREQTAG,(reducer,fname))
        if self.debug_ == 1: sys.stderr.write("reduce step\n")
        #receives file names from reducers and concatinates them
        fout = open(self.out_file,"w")
        rednum_rest = len(self.reducers())
        while rednum_rest != 0:
            status = mpi.world.iprobe(tag=self.REDOUTTAG)
            while status is None:
                time.sleep(0.1)
                status = mpi.world.iprobe(tag=self.REDOUTTAG)
            reducer = status.source
            rednum_rest -= 1
            if self.debug_ == 1: sys.stderr.write("reducer %d -> master (reduced data)\n" % reducer)
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
                while content != "":
                    fout.write(content)
                    content = mpi.world.recv(reducer,self.REDFILETAG)
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
        self.shuffled_fobj = {}
        self.shuffled_lines = {}
        self.shuffled_filenames = {}
        for reducer in self.reducers():
            rkey = str(reducer)
            self.shuffled_filenames[rkey] = []
            self.shuffled_lines[rkey] = 0
        #receives data from master
        fname_in = self.temp_dir+"/"+self.getID()+"map.txt"
        valid_spouts = self.spouts()[:]
        random.shuffle(valid_spouts) #decentralization
        spout_idx = 0
        if self.map_from_file == 1:
            # received data stored in a file
            fin = open(fname_in,"w")
            while True:
                source = valid_spouts[spout_idx]
                mpi.world.send(source,self.MAPREQTAG,"")
                msg = mpi.world.recv(source,self.MAPINTAG)
                if msg != "EOF":
                    for line in msg:
                        fin.write(line+"\n")
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
                mpi.world.send(source,self.MAPREQTAG,"")
                msg = mpi.world.recv(source,self.MAPINTAG)
                if msg != "EOF":
                    for line in msg:
                        try:
                            self.map(json.loads(line))
                        except Exception as inst:
                            self.setError(inst.args[0])
                            self.checkErrorCount()
                else:
                    valid_spouts.remove(source)
                    if len(valid_spouts) == 0: break
                spout_idx = (spout_idx + 1) % len(valid_spouts)
        #finishes writing files
        for fobj in self.shuffled_fobj.values():
            fobj.close()
        #sorts shuffled files
        for rkey in self.shuffled_filenames.keys():
            for fname in self.shuffled_filenames[rkey]:
                data = []
                for line in open(fname,"r"):
                    try:
                        data.append(json.loads(line.rstrip()))
                    except Exception as inst:
                        self.setError(inst.args[0])
                        self.checkErrorCount()
                f = open(fname,"w")
                for row in sorted(data,key=itemgetter(0)):
                    f.write(json.dumps(row)+"\n")
                f.close()
        data = None #frees memory
        #send file list to master node
        mpi.world.send(0,self.MAPOUTTAG,self.shuffled_filenames)
        #if a file is requested, send it to a reducer
        msg = mpi.world.recv(0,self.MAPFILEREQTAG)
        while msg != "":
            (reducer,fname) = msg
            content = open(fname,"r").read()
            mpi.world.send(reducer,self.MAPFILETAG,content)
            content = ""
            # to reduce probing during wait
            status = mpi.world.iprobe(tag=self.MAPFILEREQTAG)
            while status is None:
                time.sleep(0.01)
                status = mpi.world.iprobe(tag=self.MAPFILEREQTAG)
            msg = mpi.world.recv(0,self.MAPFILEREQTAG)
        #deletes temporary files
        tempfiles = [fname_in]
        for files in self.shuffled_filenames.values():
            tempfiles += files
        for fname in tempfiles:
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
        status = mpi.world.iprobe(tag=self.REDINTAG)
        while status is None:
            time.sleep(0.1)
            status = mpi.world.iprobe(tag=self.REDINTAG)
        files = mpi.world.recv(0,self.REDINTAG)
        #copies file if it doesn't exist
        reqfiles = {}
        for mkey in files.keys():
            for fname in files[mkey]:
                if not os.path.exists(fname):
                    if not reqfiles.has_key(mkey):
                        reqfiles[mkey] = []
                    reqfiles[mkey].append(fname)
        mpi.world.send(0,self.MAPFILEREQTAG,reqfiles)
        for mkey in reqfiles.keys():
            for fname in reqfiles[mkey]:
                f = open(fname,"w")
                content = mpi.world.recv(int(mkey),self.MAPFILETAG)
                f.write(content)
                f.close()
        
        #outputs file
        fname_out = self.temp_dir+"/"+self.getID()+"_red.txt"
        self.reducer_file = open(fname_out,"w")
        #opens files
        fobjs = []
        for mkey in files.keys():
            for fname in files[mkey]:
                fobjs.append(open(fname,"r"))
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
                            self.setError(str(type(inst))+str(inst.args[0])+" during reading: "+nextline)
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
                        self.setError(str(type(inst))+str(inst.args[0])+" during processing key = %s" % (key_prev))
                        self.checkErrorCount()
                key_prev = kmin
                vals = [vmin]
            lines[fmin] = "" #for next loop
        #finishing reduce step
        if len(vals) != 0:
            try:
                self.reduce((key_prev,vals))
            except Exception as inst:
                self.setError(str(type(inst))+str(inst.args[0])+" during processing key = %s" % (key_prev))
                self.checkErrorCount()
        self.reducer_file.close()
        #sends result to master node
        mpi.world.send(0,self.REDOUTTAG,fname_out)
        msg = mpi.world.recv(0,self.REDFILEREQTAG)
        if msg != "":
            content = ""
            red_lines = 0
            for line in open(fname_out,"r"):
                content += line
                red_lines += 1
                if red_lines >= self.reduced_line_max:
                    mpi.world.send(0,self.REDFILETAG,content)
                    content = ""
                    red_lines = 0
            if content != "" : mpi.world.send(0,self.REDFILETAG,content)
            mpi.world.send(0,self.REDFILETAG,"")
            # to reduce probing during wait
            status = mpi.world.iprobe(tag=self.REDFILEREQTAG)
            while status is None:
                time.sleep(0.1)
                status = mpi.world.iprobe(tag=self.REDFILEREQTAG)
            msg = mpi.world.recv(0,self.REDFILEREQTAG)
        #deletes files
        tempfiles = [fname_out]
        for fnames in files.values():
            tempfiles += fnames
        for fname in tempfiles:
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
            
