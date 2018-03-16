CXX=g++
LDFLAGS=-lpthread -lrt -lsnappy -lbz2 -lz
CXXFLAGS=-std=c++11

DEPS_PATH=./deps

ifndef ROCKSDB_PATH
ROCKSDB_PATH=$(DEPS_PATH)/rocksdb
endif
ROCKSDB=$(ROCKSDB_PATH)/librocksdb.a

ifndef BLACKWIDOW_PATH
BLACKWIDOW_PATH=$(DEPS_PATH)/blackwidow
endif
BLACKWIDOW=$(BLACKWIDOW_PATH)/lib/libblackwidow.a

INCLUDE_PATH = -I./include                    \
               -I$(BLACKWIDOW_PATH)/include   \
               -I$(ROCKSDB_PATH)/include      \

LIB_PATH     = -L$(BLACKWIDOW_PATH)/lib       \
               -L$(ROCKSDB_PATH)/             \

LIBS         = -lblackwidow                   \
               -lrocksdb                      \

.PHONY: clean all

OBJECTS= $(BLACKWIDOW) $(ROCKSDB) benchmark

all: $(OBJECTS)

$(BLACKWIDOW):
	make -C $(BLACKWIDOW_PATH) ROCKSDB_PATH=../rocksdb

$(ROCKSDB):
	make -C $(ROCKSDB_PATH) static_lib -j 24

benchmark: benchmark.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(INCLUDE_PATH) $(LIB_PATH) $(LDFLAGS) $(LIBS)

clean:
	rm -rf benchmark

distclean:
	rm -rf benchmark
	make -C $(BLACKWIDOW_PATH) clean
	make -C $(ROCKSDB_PATH) clean
