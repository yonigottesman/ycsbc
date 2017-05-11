CXX        = g++
OPT        = -O3 -DNDEBUG
CXXFLAGS   = -std=c++11 -g $(OPT) -Wall -pthread -I./
LIBPIWI    = -Wl,-rpath,'../piwi' -L../piwi
ROCKSDB_SO = librocksdb.so

ifeq ($(findstring O0, $(OPT)), O0)
	LIBPIWI += -lpiwi_d
	ROCKSDB_SO = librocksdb_debug.so
	CXXFLAGS += -fsanitize=undefined -fbounds-check
	ASAN_LIB = -lasan
else
	LIBPIWI += -lpiwi
endif
ROCKS_ROOT = ../rocksdb
LIBROCKS   = -Wl,-rpath,'$(ROCKS_ROOT)' -L$(ROCKS_ROOT) -l:$(ROCKSDB_SO)
LDFLAGS    = $(ASAN_LIB) -lpthread $(LIBPIWI) $(LIBROCKS) -fopenmp
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
	$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

