include ../../build_config.mk

OBJS = util.o encode.o decode.o
EXES = 

all: ${OBJS}
	ar -cru ./libcodec.a ${OBJS}

util.o: util.h util.cpp
	${CXX} ${CFLAGS} -c util.cpp

encode.o: encode.h encode.cpp
	${CXX} ${CFLAGS} -c encode.cpp

decode.o: decode.h decode.cpp
	${CXX} ${CFLAGS} -c decode.cpp

clean:
	rm -f ${EXES} ${OBJS} *.o *.exe *.a

