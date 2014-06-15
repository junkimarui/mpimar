mpimar
======
mpimar(MPI Map Reduce Library), pronounced as /empima:r/.

Dependencies
======
Python binding for "boost.mpi"

Usage
======
`mpimar.py` is a library source. Following sample programs are available.
* word_count.py
* inverted_index.py

To run sample programs,
`python run.py 5 3 word_count.py data/split.aa,data/split.ab,data/split.ac,data/split.ad,data/split.ae,data/split.af,data/split.ag,data/split.ah,data/split.ai,data/split.aj out.txt`
or
`python run.py 5 3 inverted_index.py data/split.aa,data/split.ab,data/split.ac,data/split.ad,data/split.ae,data/split.af,data/split.ag,data/split.ah,data/split.ai,data/split.aj out.txt`