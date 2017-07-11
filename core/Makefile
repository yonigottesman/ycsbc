CXX=g++
OPT=-O3 -DNDEBUG
BOOST_DIR=/usr/local/boost_1_61_0
CXXFLAGS=$(OPT) -std=c++11 -c -g -Wall -I ${BOOST_DIR}
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

ifeq ($(findstring O0, $(OPT)), O0)
	CXXFLAGS += -fsanitize=undefined -fbounds-check
endif

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CXX) $(CXXFLAGS) $< -o $@

clean:
	$(RM) $(OBJECTS)
