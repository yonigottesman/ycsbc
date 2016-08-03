CC=g++
OPT=-O0
CFLAGS=-std=c++11 -g $(OPT) -Wall -pthread -I./ -isystem ../cds-2.1.0 -fopenmp
#LDFLAGS= -lpthread -ltbb -lhiredis
LIBPIWI=../piwi/piwi.a
LIBROCKS=../rocksdb/librocksdb.a
LDFLAGS= -lpthread -ltbb -L../libcds/bin -lcds-s -lbz2 -lz -lsnappy $(LIBPIWI) $(LIBROCKS)
#LDFLAGS= -lpthread -ltbb —L./redis/hiredis -lhiredis
#SUBDIRS=core db redis
SUBDIRS=core db
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

OS := $(shell uname)
ifeq ($(OS),Darwin)
# Run MacOS commands 
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

