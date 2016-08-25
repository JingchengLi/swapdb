CXX = g++
CXXFLAGS = -Wall -Wno-format -DDEBUG -g -O0 -std=c++11 -D__XDEBUG__

OBJECT = main test_bgsave zset test_server hash kv_test ttl bench_hash list_lock

LIB_PATH = -L../output/lib/

LIBS = -lnemo \
	-lrocksdb \
	-lz \
	-lbz2 \
	-lsnappy \
	-lrt \
	-lpthread

INCLUDE_PATH = -I../output/include/

OBJS = main.o test_bgsave.o zset.o test_server.o test_mset.o hash.o hash_test.o kv_test.o zset_test.o set_test.o list_test.o ttl.o bench_hash.o list_lock.o

.PHONY: all clean

all: $(OBJECT)
	@echo "Success, go, go, go..."

main: main.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

zset: zset.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

list_lock: list_lock.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

ttl: ttl.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

kv_test: kv_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

hash_test: hash_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

bench_hash: bench_hash.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

hash: hash.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

zset_test: zset_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

set_test: set_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

list_test: list_test.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)
	
test_mset: test_mset.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

test_bgsave: test_bgsave.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

test_server: test_server.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(OBJECT)
	rm -f $(OBJS)

