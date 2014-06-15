#!/usr/bin/env python
import sys
import subprocess


argv = sys.argv
if len(argv) < 4:
    print "Usage: python %s [mapper number] [reducer number] [job python file] [other arguments]" % argv[0]
    quit()
mapper_num = int(argv[1])
reducer_num = int(argv[2])
job = argv[3]
argv = argv[4:]
total_num = mapper_num + reducer_num + 1
cmd = "mpirun -np %d python %s %d %d %s" % (total_num,job,mapper_num,reducer_num," ".join(argv))
print cmd 
subprocess.call(cmd,shell=True)
