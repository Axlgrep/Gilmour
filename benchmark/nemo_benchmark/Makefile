CXX=g++
LDFLAGS=-lpthread -lrt -lsnappy -lbz2 -lz -llz4
CXXFLAGS=-std=c++11

DEPS_PATH=./deps

ifndef ROCKSDB_PATH
ROCKSDB_PATH=$(DEPS_PATH)/rocksdb
endif
ROCKSDB=$(ROCKSDB_PATH)/librocksdb.a

ifndef NEMO_PATH
NEMO_PATH=$(DEPS_PATH)/nemo
endif
NEMO=$(NEMO_PATH)/lib/libnemo.a

ifndef NEMOROCKSDB_PATH
NEMOROCKSDB_PATH=$(DEPS_PATH)/nemo-rocksdb
endif
NEMOROCKSDB=$(NEMO_PATH)/lib/libnemodb.a

INCLUDE_PATH = -I./include                    \
               -I$(NEMO_PATH)/include         \
               -I$(NEMOROCKSDB_PATH)/include  \
               -I$(ROCKSDB_PATH)/include      \

LIB_PATH     = -L$(NEMO_PATH)/lib             \
               -L$(NEMOROCKSDB_PATH)/lib      \
               -L$(ROCKSDB_PATH)/             \

LIBS         = -lnemo                         \
               -lnemodb                       \
               -lrocksdb                      \

.PHONY: clean all

OBJECTS=  $(NEMO) $(NEMOROCKSDB) $(ROCKSDB) benchmark

all: $(OBJECTS)

$(NEMO):
	make -C $(NEMO_PATH) NEMODB_PATH=../nemo-rocksdb ROCKSDB_PATH=../rocksdb

$(NEMOROCKSDB):
	make -C $(NEMOROCKSDB_PATH) ROCKSDB_PATH=../rocksdb

$(ROCKSDB):
	make -C $(ROCKSDB_PATH) static_lib -j 24

benchmark: benchmark.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS) $(LDFLAGS)

clean:
	rm -rf db
	rm -rf benchmark

distclean:
	rm -rf db
	rm -rf benchmark
	make -C $(NEMO_PATH) clean
	make -C $(NEMOROCKSDB_PATH) clean
	make -C $(ROCKSDB_PATH) clean
