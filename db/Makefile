CXX=g++
OPT=-O3 -DNDEBUG
CXXFLAGS=-std=c++11 -c -g $(OPT) -Wall -fgnu-tm
#CXXFLAGS=-std=c++11 -c -g -Wall
EXT_INC_ROOT = ../..
ROCKSDB_SRC = $(EXT_INC_ROOT)/rocksdb5
ROCKSDB_INC = $(ROCKSDB_SRC)/include
PIWI_INC = $(EXT_INC_ROOT)/piwi/include
INCLUDES=-I../ -isystem $(PIWI_INC) -isystem $(ROCKSDB_SRC) -isystem $(ROCKSDB_INC)
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

