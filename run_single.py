#!/usr/bin/env python
import sys
import subprocess


argv = sys.argv
if len(argv) < 5:
    print "Usage: python %s [spout number] [mapper number] [reducer number] [job python file] [other arguments]" % argv[0]
    quit()
spout_num = int(argv[1])
mapper_num = int(argv[2])
reducer_num = int(argv[3])
job = argv[4]
argv = argv[5:]
total_num = mapper_num + reducer_num + spout_num
cmd = "mpirun -np %d python %s %d %d %d %s" % (total_num,job,spout_num,mapper_num,reducer_num," ".join(argv))
print cmd
subprocess.call(cmd,shell=True)
