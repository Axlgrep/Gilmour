CXX=g++
LDFLAGS=-lpthread -lrt
CXXFLAGS=-std=c++11

THIRD_PATH=../third

ifndef NEMO_PATH
NEMO_PATH=$(THIRD_PATH)/nemo
endif
NEMO=$(NEMO_PATH)/lib/libnemo.a

ifndef BLACKWIDOW_PATH
BLACKWIDOW_PATH=$(THIRD_PATH)/blackwidow
endif
BLACKWIDOW=$(BLACKWIDOW_PATH)/lib/libblackwidow.a

ifndef ROCKSDB_PATH
ROCKSDB_PATH=$(THIRD_PATH)/rocksdb
endif

INCLUDE_PATH = -I./include                  \
               -I$(NEMO_PATH)/include       \
               -I$(BLACKWIDOW_PATH)/include \
               -I$(ROCKSDB_PATH)/include    \

LIB_PATH     = -L$(NEMO_PATH)/lib           \
               -L$(BLACKWIDOW_PATH)/lib     \

LIBS         = -lnemo                       \
               -lblackwidow                 \

.PHONY: clean all

OBJECTS= $(NEMO) $(BLACKWIDOW) engine_benchmark

all: $(OBJECTS)

$(NEMO):
	make -C $(NEMO_PATH) ROCKSDB_PATH=../rocksdb/include/

$(BLACKWIDOW):
	make -C $(BLACKWIDOW_PATH)

engine_benchmark: engine_benchmark.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(INCLUDE_PATH) $(LIB_PATH) $(LDFLAGS) $(LIBS)

clean:
	rm -rf engine_benchmark

distclean:
	rm -rf engine_benchmark
	make -C $(NEMO_PATH) clean
	make -C $(BLACKWIDOW_PATH) clean
