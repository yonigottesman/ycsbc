CC         = g++
OPT        = -O0
CFLAGS     = -std=c++11 -g $(OPT) -Wall -pthread -I./ -isystem ../cds-2.1.0 -fopenmp
#LDFLAGS    = -lpthread -ltbb -lhiredis
LIBPIWI    = -Wl,-rpath,'../piwi' -L../piwi -lpiwi
#LIBPIWI   = ../piwi/piwi.a
ROCKSDB_SO = librocksdb.so
LIBROCKS   = -Wl,-rpath,'../rocksdb' -L../rocksdb -l:$(ROCKSDB_SO)
#LIBROCKS   = ../rocksdb/librocksdb.a ../rocksdb/libbz2.a ../rocksdb/libz.a ../rocksdb/libsnappy.a 
#LDFLAGS   = -Wl,-rpath,'../rocksdb' -Wl,-rpath-link,../rocksdb -lpthread -ltbb -L../libcds/bin -lcds-s $(LIBPIWI) $(LIBROCKS)
LDFLAGS    = -lpthread -ltbb -Wl,-rpath,'../libcds/bin' -L../libcds/bin -lcds $(LIBPIWI) $(LIBROCKS)
#LDFLAGS    = -lpthread -ltbb —L./redis/hiredis -lhiredis
#SUBDIRS    = core db redis
SUBDIRS    = core db
SUBSRCS    = $(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS    = $(SUBSRCS:.cc=.o)
EXEC       = ycsbc

LD_LIBRARY_PATH = /usr/lib/x86_64-linux-gnu

OS := $(shell uname)
ifeq ($(OS),Darwin)
# Run MacOS commands 
	CXX=/usr/local/Cellar/gcc/6.1.0/bin/g++
	AR = libtool
	ARFLAGS = -static -o
else
# check for Linux and run other commands
	LDFLAGS += -lrt
endif

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

