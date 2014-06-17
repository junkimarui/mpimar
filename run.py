#!/usr/bin/env python
import sys
import os
import subprocess

argv = sys.argv
if len(argv) < 5:
    print "Usage: python %s [host file] [mapper number] [reducer number] [job python file] [other arguments]" % argv[0]
    quit()
host_file = argv[1]
mapper_num = int(argv[2])
reducer_num = int(argv[3])
job = argv[4]
argv = argv[5:]
total_num = mapper_num + reducer_num + 1
#send the same program to remote hosts
cwd = os.getcwd()
programs = ["mpimar.py",job]
for line in open(host_file,"r"):
    row = line.split(" ")
    host = row[0]
    if host == "localhost": continue
    if host == "": continue
    for program in programs:
        cmd = "ssh %s mkdir %s" % (host,cwd)
        print cmd
        subprocess.call(cmd,shell=True)
        cmd = "scp %s/%s %s:%s/" %(cwd,program,host,cwd)
        print cmd
        subprocess.call(cmd,shell=True)

cmd = "mpirun --hostfile %s -np %d python %s %d %d %s" % (host_file,total_num,job,mapper_num,reducer_num," ".join(argv))
print cmd 
subprocess.call(cmd,shell=True)
