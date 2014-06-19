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
    #args = {"name":"NAME", "temp_dir":"TEMP_DIR", "mapper":0.5, "reducer":0.5, "out_file":"OUTPUT FILE", "allow_error_num":0}
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
        self.allow_error_num = args["allow_error_num"] if args.has_key("allow_error_num") else 0
        self.error_ = []
        self.emit_idx = 0 #for master

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
    def getReducerFromKey(self,key): return self.reducers()[hash(key) % self.reducer_num]
    def emit(self,obj):
        if self.isMaster():
            self.emit_idx = (self.emit_idx + 1) % self.mapper_num
            mpi.world.send(self.emit_idx+1,self.MAPINTAG,json.dumps(obj))
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
        
    def master(self):
        self.distribute()
        #tells mappers to finish receiving data
        for mapper in self.mappers():
            mpi.world.send(mapper,self.MAPINTAG,"EOF")
        #receives file names from mappers
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
            mpi.world.send(int(rkey), self.REDINTAG, files[rkey])

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
                    self.setError(fname+" might not be deleted")
            else:
                mpi.world.send(reducer,self.REDFILEREQTAG,fname)
                content = mpi.world.recv(reducer,self.REDFILETAG)
                fout.write(content)
                content = ""

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
        #receives data from master
        fname_in = self.temp_dir+"/"+self.getID()+"map.txt"
        fin = open(fname_in,"w")
        msg = mpi.world.recv(0,self.MAPINTAG)
        while msg != "EOF":
            fin.write(msg+"\n")
            msg = mpi.world.recv(0,self.MAPINTAG)
        fin.close()
        #map step (and shuffle at the same time)
        ##open files
        self.shuffled_files = {}
        filenames = {}
        for reducer in self.reducers():
            rkey = str(reducer)
            filenames[rkey] = self.temp_dir+"/"+self.getID()+"mr_"+rkey+".txt"
            self.shuffled_files[rkey] = open(filenames[rkey],"w")
        line_num = 0
        for line in open(fname_in,"r"):
            try:
                self.map(json.loads(line.rstrip()))
            except Exception as inst:
                self.setError(inst.args[0]+" at line "+ str(line_num))
                self.checkErrorCount()
            line_num += 1
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
                    self.setError(fname+" might not be deleted")
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
                        lines[fobj] = json.loads(lines[fobj].rstrip())
                    else:
                        #deletes if it's already read
                        fobjs.remove(fobj)
                        del(lines[fobj])
            #if it finished reading all the files
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
                   self.setError(fname+" might not be deleted")
        mpi.world.send(0,self.FINISHTAG,self.error_)

    def start(self):
        if self.isMaster():
            self.master()
        elif self.isMapper():
            self.mapper()
        else:
            self.reducer()
            
