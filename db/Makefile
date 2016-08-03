CC=g++
OPT=-O0
CFLAGS=-std=c++11 -c -g $(OPT) -Wall -fgnu-tm -isystem ../../cds-2.1.0
#CFLAGS=-std=c++11 -c -g -Wall
INCLUDES=-I../ -I../../piwi/include -I../../rocksdb/include
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

