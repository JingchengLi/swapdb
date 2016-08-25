GCC = g++
CPPFLAGS = -Wall -W -Wno-unused-parameter -DDEBUG -D__XDEBUG__ -g -O2 -std=c++11
OBJECT = meta_scan

LIB_PATH = -L ../../output/lib
			
LIBS = -Wl,-Bstatic -lnemo -lrocksdb\
	   -Wl,-Bdynamic -lpthread\
	   -lsnappy \
	   -lrt \
	   -lz \
	   -lbz2

INCLUDE_PATH = -I../../include/ \
							 -I../../3rdparty/rocksdb/include

.PHONY: all clean


# BASE_BOJS := $(wildcard *.cpp)
# BASE_BOJS += $(wildcard *.c)
# OBJS := $(patsubst %.cpp,%.o,$(BASE_BOJS)) 


all: $(OBJECT)
	rm *.o

$(OBJECT): $(OBJECT).o
	$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

%.o : %.cc
	$(GCC) $(CPPFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean:
	rm -rf $(OBJECT) $(OBJECT).o
