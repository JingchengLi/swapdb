#问题：若是用1.6.0的include，用命令g++ -I./include -g -Wall -c sample.cc 编不过，但是用1.7.0的include就没有问题,why？

NOUSE := $(shell sh -c 'cd gtest_1.7.0/ && make static_lib')
#release_hdr := $(shell sh -c './mkreleasehdr.sh')
CXX = g++
CXXFLAGS = -Wall -DDEBUG -g -O0 -std=c++11 -D__XDEBUG__
#CXXFLAGS = -g -Wall -Wextra -std=c++11
OBJECT = nemo_all_test nemo_kv_test nemo_hash_test nemo_list_test nemo_set_test nemo_zset_test

LIB_PATH = -L../output/lib/ -L./gtest_1.7.0/lib/\

LIBS = -lpthread \
	   -lnemo \
	   -lrocksdb \
	   -lz \
	   -lbz2 \
	   -lsnappy \
	   -lrt \
	   -lgtest_main 


INCLUDE_PATH = -I../output/include/ -I./include -I./gtest_1.7.0/include

OBJS = main.o nemo_zset_test.o nemo_set_test.o nemo_list_test.o nemo_hash_test.o nemo_kv_test.o

.PHONY: all clean

all: $(OBJECT)
	rm *.o
	@echo success, go, go, go...
nemo_all_test: $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

nemo_kv_test: main.o nemo_kv_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

nemo_hash_test: main.o nemo_hash_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

nemo_list_test: main.o nemo_list_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

nemo_set_test: main.o nemo_set_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

nemo_zset_test: main.o nemo_zset_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean:
	rm -rf $(OBJECT)
	rm -rf $(OBJS)
	rm -rf tmp
	make -C gtest_1.7.0 clean
