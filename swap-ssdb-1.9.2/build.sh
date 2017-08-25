#!/bin/sh
BASE_DIR=`pwd`
JEMALLOC_PATH="$BASE_DIR/deps/jemalloc"
SNAPPY_PATH="$BASE_DIR/deps/snappy"
ROCKSDB_PATH="$BASE_DIR/deps/rocksdb"
BZ2_PATH="$BASE_DIR/deps/bzip2-1.0.6"
GFLAGS_PATH="$BASE_DIR/deps/gflags"
ZLIB_PATH="$BASE_DIR/deps/zlib"

# dependency check
which autoconf > /dev/null 2>&1
if [ "$?" -ne 0 ]; then
	echo ""
	echo "ERROR! autoconf required! install autoconf first"
	echo ""
	exit 1
fi

if test -z "$TARGET_OS"; then
	TARGET_OS=`uname -s`
fi
if test -z "$MAKE"; then
	MAKE=make
fi
if test -z "$CC"; then
	CC=gcc
fi
if test -z "$CXX"; then
	CXX=g++
fi

case "$TARGET_OS" in
    Darwin)
        #PLATFORM_CLIBS="-pthread"
		#PLATFORM_CFLAGS=""
        ;;
    Linux)
        PLATFORM_CLIBS="-pthread -lrt"
        ;;
    OS_ANDROID_CROSSCOMPILE)
        PLATFORM_CLIBS="-pthread"
        SNAPPY_HOST="--host=i386-linux"
        ;;
    CYGWIN_*)
        PLATFORM_CLIBS="-lpthread"
        ;;
    SunOS)
        PLATFORM_CLIBS="-lpthread -lrt"
        ;;
    FreeBSD)
        PLATFORM_CLIBS="-lpthread"
		MAKE=gmake
        ;;
    NetBSD)
        PLATFORM_CLIBS="-lpthread -lgcc_s"
        ;;
    OpenBSD)
        PLATFORM_CLIBS="-pthread"
        ;;
    DragonFly)
        PLATFORM_CLIBS="-lpthread"
        ;;
    HP-UX)
        PLATFORM_CLIBS="-pthread"
        ;;
    *)
        echo "Unknown platform!" >&2
        exit 1
esac

cd "$DIR"
DIR=`pwd`
cd $SNAPPY_PATH
echo ""
echo "##### building snappy... #####"
cmake .
make snappy
echo "##### building snappy finished #####"
echo ""

cd "$DIR"
DIR=`pwd`
cd $BZ2_PATH
make bzip2
	

cd "$DIR"
DIR=`pwd`
cd $GFLAGS_PATH
if [ ! -f Makefile ];then
	cmake .
	make
fi


cd "$DIR"

case "$TARGET_OS" in
	CYGWIN*|FreeBSD|OS_ANDROID_CROSSCOMPILE)
		echo "not using jemalloc on $TARGET_OS"
	;;
	*)
		DIR=`pwd`
		cd $JEMALLOC_PATH
		if [ ! -f Makefile ]; then
			echo ""
			echo "##### building jemalloc... #####"
			sh ./autogen.sh
			./configure
			make
			echo "##### building jemalloc finished #####"
			echo ""
		fi
		cd "$DIR"
	;;
esac

cd "$DIR"
DIR=`pwd`
cd $ROCKSDB_PATH
if [ -f Makefile ];then
    echo "##### building rocksdb... #####"
    make -j10 OPT="-DSNAPPY -I$SNAPPY_PATH -L$SNAPPY_PATH -DJEMALLOC -I$JEMALLOC_PATH/include -L$JEMALLOC_PATH/lib -L$ZLIB_PATH" static_lib
    echo "##### building rocksdb finished #####"
fi


cd "$DIR"
DIR=`pwd`
cd $ZLIB_PATH
echo "##### building zlib... #####"
./configure
make
echo "##### building zlib finished #####"


cd "$DIR"
rm -f src/version.h


echo "#ifndef SSDB_DEPS_H" >> src/version.h
echo "#ifndef SSDB_VERSION" >> src/version.h
echo "#define SSDB_VERSION \"`cat version`\"" >> src/version.h
echo "#endif" >> src/version.h
echo "#define GIT_BUILD \"`git rev-parse HEAD 2>/dev/null`\"" >> src/version.h
echo "#define BUILD_DATE \"`date +%Y-%m-%dT%H:%M:%S-%Z`\"" >> src/version.h

echo "#endif" >> src/version.h
case "$TARGET_OS" in
	CYGWIN*|FreeBSD)
	;;
	OS_ANDROID_CROSSCOMPILE)
        echo "#define OS_ANDROID 1" >> src/version.h
	;;
	*)

	echo "#include <jemalloc/jemalloc.h>" >> src/version.h
	;;
esac

rm -f build_config.mk
echo CC=$CC >> build_config.mk
echo CXX=$CXX >> build_config.mk
echo "MAKE=$MAKE" >> build_config.mk
echo "JEMALLOC_PATH=$JEMALLOC_PATH" >> build_config.mk
echo "SNAPPY_PATH=$SNAPPY_PATH" >> build_config.mk

echo "CFLAGS=" >> build_config.mk
echo "CFLAGS = -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare" >> build_config.mk
echo "CFLAGS += ${PLATFORM_CFLAGS}" >> build_config.mk

echo "CLIBS=" >> build_config.mk
echo "CLIBS += \"$SNAPPY_PATH/libsnappy.a\"" >> build_config.mk

case "$TARGET_OS" in
	CYGWIN*|FreeBSD|OS_ANDROID_CROSSCOMPILE)
	;;
	*)
		echo "CLIBS += \"$JEMALLOC_PATH/lib/libjemalloc.a\"" >> build_config.mk
		echo "CFLAGS += -I \"$JEMALLOC_PATH/include\"" >> build_config.mk
	;;
esac

echo "CLIBS += ${PLATFORM_CLIBS}" >> build_config.mk


if test -z "$TMPDIR"; then
    TMPDIR=/tmp
fi

g++ -x c++ - -o $TMPDIR/ssdb_build_test.$$ 2>/dev/null <<EOF
	#include <unordered_map>
	int main() {}
EOF
if [ "$?" = 0 ]; then
	echo "CFLAGS += -DNEW_MAC" >> build_config.mk
fi

