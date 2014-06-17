mpimar
======
mpimar(MPI Map Reduce Library), pronounced as /empima:r/.

Dependencies
------
Python binding for "boost.mpi"

Usage
------
`mpimar.py` is a library source. Following sample programs are available.
* word_count.py
* inverted_index.py

To run sample programs,  
```
python run_single.py 5 3 word_count.py data/split.aa,data/split.ab,data/split.ac,data/split.ad,data/split.ae,data/split.af,data/split.ag,data/split.ah,data/split.ai,data/split.aj out.txt
```
or
```
python run_single.py 5 3 inverted_index.py data/split.aa,data/split.ab,data/split.ac,data/split.ad,data/split.ae,data/split.af,data/split.ag,data/split.ah,data/split.ai,data/split.aj out.txt
```  
If you set up MPI environment, you could use run.py instead.
```
python run.py 5 3 hostfile inverted_index.py data/split.aa,data/split.ab,data/split.ac,data/split.ad,data/split.ae,data/split.af,data/split.ag,data/split.ah,data/split.ai,data/split.aj out.txt
```
run.py sends required programs to the exact same directory on remote servers.

How to set up MPI environment
-----------
Here, I briefly describe how to set up MPI environment.  
First, install Open MPI on all the servers.  Open MPI 1.4.x and 1.6.x are not compatible so you should use the same version of Open MPI throughout the servers. This program requires python bindings for boost.mpi so you should also set this up. (Ubuntu: `apt-get install libboost-mpi-python-dev`)  
Second, let Open MPI connect other servers via SSH without passphrase. You could make pubkey without passphrase.  
Third, check the PATH and LD_LIBRARY_PATH. If you type `ssh [remote server] env|grep -i path` and you cannot see proper PATH and LD_LIBRARY_PATH, you could use `.ssh/environment` (and you should add `PermitUserEnvironment=yes` to `sshd_config`).

Development Environment
-----------
OS: Ubuntu 14.04  
CPU: Xeon E5-2650 v2  
Python version: 2.7.6  
MPI version: Open MPI 1.6.5  
Boost MPI version: 1.54  

COPYRIGHT
-----------
2014 Junki Marui  
This software is released under the MIT License.  
http://opensource.org/licenses/mit-license.php 
