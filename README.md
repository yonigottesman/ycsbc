# YCSB-C

Yahoo! Cloud Serving Benchmark in C++, a C++ version of YCSB (https://github.com/brianfrankcooper/YCSB/wiki)

## Quick Start

To build YCSB-C on Ubuntu, for example:

```
$ sudo apt-get install libtbb-dev
$ make
```

## Piwi project additions

The original project contained various irrelevant benchmarks (tbb, redis etc.), which were removed to reduce dependencies and keep things simple. On the other hand, 4 new DBs were added:
 1. piwi - running a single instance of Piwi
 2. rocks - running a single instance of RocksDB
 3. multi_rocks - running multiple instances of RocksDB, sharded by the end of the keys (e.g., for 10 shards, key user...7 will be directed to shard #7)
 4. stats - simulate piwi run but only output statistics regarding key distribution and estimated munk hit rate
 
### Build
#### Prerequisites
 - Piwi library (exact one depends on YCSB-C build)
 - RocksDB library (ditto)
 - Boost accumulate
 - Developed using GCC 5.4, any higher version should also work

#### Build targets
There's only one build target (all), producing the `ycsbc` executable. By default, a fully optimized executable is created. This executable links to libpiwi.so and librocksdb.so, which are also optimized builds.

To obtain a debug build, set the makefiles OPT variable to O0 (e.g., by building using `make OPT=O0`). The created executable will be linked against libpiwi_d.so and librocksdb_debug.so, which must be built separately before.

Note that it is highly recommended to run `make clean` before switching between debug and release builds, otherwise the resulting ycsbc executable might be composed by object files coming from either setting, yielding very strange behavior.

**FIXME:** for some reason, the build sometimes complains it can't find Piwi and RocksDB interface headers. Running the exact build command again will succeed, so this isn't a show-stopper. Something is misconfigured in the makefiles, not sure what.

### Code structure
YCSB-C code is divided in 3 folders. Note that C++ source files use the .cc extension, as opposed to .cpp used in Piwi - when adding files, use the .cc extension, otherwise the current makefile won't build those files.

#### Base (installation) folder
ycsbc.cc - contains `main`, parses arguments, measures timing and prints statistics
Makefile - the makefile building the whole program (other folders have makefiles handling their code)
runTests.py - convenience script, running multiple benchmarks with multiple options; parses results and summarises.

#### core
client - abstraction of a user, running various operations on the given DB. Note that we do not deal with value fields, and always consider the first (and only) result provided.
core_workload - sets up workload parameters and generates keys and values accordingly.
various generators - creates different key distributions:
 - uniform, zipfian, latest, scrambled: see [original YCSB paper](https://pdfs.semanticscholar.org/9aa0/d7253574e50fe3a190ccd924433f048997dd.pdf), section 4.1.
 Note that vanilla zipfian is extremely skewed. OTOH, hashing the zipfian distribution ("scrambled zipfian") evenly spreads hot keys across the key range, making load distribution fairly uniform on a munk/block granularity.
 - complex zipfian: the first (higher) part of the key is generated using vanilla zipf, the second using hashed zipf. This overloads a part of the DB, but within the overloaded area, load is distributed in a fairly uniform way due to hashing.
 - flurry: similar to complex zipfian, but a fixed padding is placed between the zipf parts, further reducing distribution. this should resemble distribution of Flurry keys, which are created by concatenating a few properties.

#### db
db_factory - gets a db name and creates a matching db object
*_db - various db implementations

### Execution arguments
In general, arguments can be provided using a file (passed after the -P flag), in the command line or both (the command line args override those in the file). File format is <arg>=<value>, command line equivalent is -<arg> <value>. Currently supported (and used) arguments:

#### General
 - fieldlength: size of generated value (content is a random character * size)
 - exact_key_size: size of generated key
 - readproportion: ratio of gets in the workload
 - updateproportion: ratio of puts updating existing keys
 - insertproportion: ratio of puts introducing new keys
 - scanproportion: ratio of scans
 - maxscanlength: range of keys to obtain in a scan
 - requestdistribution: key distribution
 - recordcount: initial number of keys to insert into the DB (timing and stats not measured)
 - operationcount: actual workload size (timing and stats are measured)
 - validateGets: make sure returned values are correct (done by placing the key inside the value; use for debug only)
Other fields that aren't currently in use but may come in handy (e.g., insertstart) can be found in core_workload.cc, along with their defaults

#### Piwi
See Piwi documentation (all YCSB-C arguments are passed to Piwi, which picks the ones of interest)

#### RocksDB
rocksdb_usefsync - if set (true, yes or 1), every write to a file is immediately flushed to disk.
rocksdb_syncwrites - similar, but apparently slightly more lax. This is what we use.
rocksdb_cachesize - the amound of RAM (in bytes) RocksDB can use to cache file blocks. Doesn't seem to have any effect...
compression - whether to compress sst files. Currently disabled (arg - off/no/false) to match Piwi. When Piwi enables compression, should effect it as well.

### TODO
 - Add support for starting with an existing DB (have new inserts start from the end of the existing range and random keys drawn from till the end of that range).
 - Add timeline stats, namely trends over time. Note that YCSB-C gets only "externally visible" information - duration of operations, OS information such as I/O activity, RAM usage etc. Finer details (e.g., rebalancing) require DB cooperation.