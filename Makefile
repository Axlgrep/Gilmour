CXX=g++
LDFLAGS=-lpthread -lsnappy
CXXFLAGS=-std=c++11
TARGET=gilmour

SRC_DIR=./src
THIRD_PATH=./third

ifndef LEVELDB_PATH
LEVELDB_PATH=$(THIRD_PATH)/leveldb
endif
LEVELDB=$(LEVELDB_PATH)/out-static/libleveldb.a

INCLUDE_PATH = -I./include               \
               -I$(LEVELDB_PATH)/include \

LIB_PATH = -L$(LEVELDB_PATH)/out-static/ \

LIBS = -lleveldb                         \

SOURCE := $(wildcard $(SRC_DIR)/*.cc)
OBJS := $(patsubst %.cc, %.o, $(SOURCE))

default: all

all: $(TARGET)

# 这里的$(OBJS)不能放在最后，CSAPP中说过
# 链接器维持了一个可重定位目标文件的集合
# E, 一个未解析的符号集合U,以及一个在前面
# 输入文件中已经定义的符号集合D...
$(TARGET): $(LEVELDB) $(OBJS)
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(LEVELDB):
	make -C $(LEVELDB_PATH)

# <targets..> : <target-pattern> : <prereq-patterns...>
#  目标文件   :    目标集模式    :    目标的依赖模式
#  $(OBJS)    :       %.o        :        %.cc
# 主要是通过编译器将%.cc文件编译成%.o可重定向文件
# $< 表示所依赖的目标集,表示取target-pattern模式中
# 的%(也就是去掉.o这个后缀), 并为其加上.cc后缀
# $@ 表示目标集，表示main.o, fun.o这些...
$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

.PHONY: clean distclean

clean:
	rm -rf $(OBJS)
	rm -rf $(TARGET)

distclean:
	rm -rf $(OBJS)
	rm -rf $(TARGET)
	make -C $(LEVELDB_PATH) clean

