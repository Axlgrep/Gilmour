CXX=g++
LDFLAGS=-lpthread -lrt -lsnappy -lbz2 -lz
CXXFLAGS=-std=c++11

DEPS_PATH=./deps

ifndef ROCKSDB_PATH
ROCKSDB_PATH=$(DEPS_PATH)/rocksdb
endif
ROCKSDB=$(ROCKSDB_PATH)/librocksdb.a

ifndef SLASH_PATH
SLASH_PATH=$(DEPS_PATH)/slash
endif
SLASH=$(SLASH_PATH)/slash/lib/libslash.a

ifndef BLACKWIDOW_PATH
BLACKWIDOW_PATH=$(DEPS_PATH)/blackwidow
endif
BLACKWIDOW=$(BLACKWIDOW_PATH)/lib/libblackwidow.a


INCLUDE_PATH = -I./include                    \
               -I$(BLACKWIDOW_PATH)/include   \
               -I$(ROCKSDB_PATH)/include      \
               -I$(SLASH_PATH)/               \

LIB_PATH     = -L$(BLACKWIDOW_PATH)/lib       \
               -L$(ROCKSDB_PATH)/             \
               -L$(SLASH_PATH)/slash/lib      \

LIBS         = -lblackwidow                   \
               -lrocksdb                      \
               -lslash                        \

.PHONY: clean all

OBJECTS= $(BLACKWIDOW) $(ROCKSDB) $(SLASH) benchmark

all: $(OBJECTS)

$(BLACKWIDOW):
	make -C $(BLACKWIDOW_PATH) ROCKSDB_PATH=../rocksdb SLASH_PATH=../slash

$(ROCKSDB):
	make -C $(ROCKSDB_PATH) static_lib
$(SLASH):
	make -C $(SLASH_PATH)/slash

benchmark: benchmark.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS) $(LDFLAGS)

clean:
	rm -rf db
	rm -rf benchmark

distclean:
	rm -rf db
	rm -rf benchmark
	make -C $(BLACKWIDOW_PATH) clean
	make -C $(ROCKSDB_PATH) clean
	make -C $(SLASH_PATH)/slash clean
