#!/bin/bash
TCL_VERSIONS="8.5 8.6"
TCLSH=""

for VERSION in $TCL_VERSIONS; do
	TCL=`which tclsh$VERSION 2>/dev/null` && TCLSH=$TCL
done

if [ -z $TCLSH ]
then
    echo "You need tcl 8.5 or newer in order to run the Redis test"
    exit 1
fi
mkdir -p ../jenkins/report/integration
cd tests/qa/integration/tcl;$TCLSH test_helper.tcl $*
