#!/usr/bin/env python
import sys
import os
import subprocess

argv = sys.argv
if len(argv) < 5:
    print "Usage: python %s [host file] [spout number] [mapper number] [reducer number] [job python file] [other arguments]" % argv[0]
    quit()
host_file = argv[1]
spout_num = int(argv[2])
mapper_num = int(argv[3])
reducer_num = int(argv[4])
job = argv[5]
argv = argv[6:]
total_num = spout_num + mapper_num + reducer_num
#send the same program to remote hosts
cwd = os.getcwd()
programs = ["mpimar.py",job]
for line in open(host_file,"r"):
    line = line.rstrip()
    row = line.split(" ")
    host = row[0]
    if host == "localhost": continue
    if host == "": continue
    #create directory
    cmd = "ssh %s mkdir -p %s" % (host,cwd)
    print cmd
    subprocess.call(cmd,shell=True)
    for program in programs:
        cmd = "scp %s/%s %s:%s/" %(cwd,program,host,cwd)
        print cmd
        subprocess.call(cmd,shell=True)

cmd = "mpirun --hostfile %s -np %d python %s %d %d %d %s" % (host_file,total_num,job,spout_num,mapper_num,reducer_num," ".join(argv))
print cmd 
subprocess.call(cmd,shell=True)
