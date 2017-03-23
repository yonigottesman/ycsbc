CXX=g++
OPT=-O3 -DNDEBUG
CXXFLAGS=-std=c++11 -c -g $(OPT) -Wall -fgnu-tm
#CXXFLAGS=-std=c++11 -c -g -Wall
ROCKSDB_ROOT = ../../rocksdb5
INCLUDES=-I../ -isystem ../../piwi/include -isystem $(ROCKSDB_ROOT)/include
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

ifeq ($(findstring O0, $(OPT)), O0)
	CXXFLAGS += -fsanitize=undefined -fbounds-check
#	CXXFLAGS += -fno-sanitize-recover
#	CXXFLAGS += -D_GLIBCXX_DEBUG
endif

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CXX) $(CXXFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

