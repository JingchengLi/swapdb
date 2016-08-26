# nemo

[![Build Status](https://travis-ci.org/Qihoo360/nemo.svg?branch=master)](https://travis-ci.org/Qihoo360/nemo)

A library that provide multiply data structure. Such as map, hash, list, set. We
build these data structure base on rocksdb


## Performance

We test the nemo library simply. We run set/get 100,000 times with a 13 bytes key and value.

    CPU: 24 * Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz

### 1. nemo 

set： 4.18 micros/op;     239,140 ops/s
get :  1.12 micros/op;     894,398 ops/s

### 2.  leveldb

set:  1.36 micros/op;    733,896 ops/s
get:  0.93 micros/op;  1,072,282 ops/s

### 3.  rocksdb

set:  3.85 micros/op;    259,599 ops/s
get:  1.00 micros/op;    998,631 ops/s

### TODO
