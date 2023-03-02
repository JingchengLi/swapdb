## SwapDB

A Redis-compatible storage solution that enables data exchange between memory and disk, allowing for significant savings in memory costs compared to Redis. The core concept behind SwapDB is to store frequently accessed "hot" keys in Redis memory, while infrequently accessed "cold" keys are stored on disk. As keys transition from cold to hot, they are loaded into Redis memory, and when keys transition from hot to cold, they are transferred back to disk. By leveraging the capabilities of SwapDB, users can achieve both high-performance caching and high-capacity key-value storage.

## Documentation

[中文文档](https://github.com/JRHZRD/swapdb/wiki/1.-swapdb%E4%BB%8B%E7%BB%8D)

## Fundamental

![fundamental](./docs/fundamental.jpg)

## Features

* Heat statistics of keys(LFU)
* Configurable threshold of RAM/FLASH capacity
* Redis API compatiable(99%). supports data structures such as strings, hashes, lists, sets, sorted sets
* Cluster management(redis cluster)
* Multiple replica nodes and data replication support(RDB+Snapshot)
* Data persistency support
* High performance and high capacity redis-like storage

## Compile

### requirements:  
```
CMake >= 3.1
GCC >= 4.8
```

### Get the source code
```
git clone https://github.com/JRHZRD/swapdb.git --recursive
```

## Let's build

(you can skip this step if you add '--recursive' option when 'git clone'.) for submodules update process.
```
git submodule update --init --recursive
```

```
cmake . && make -j8
```

## Quick start

you can quickly start a swap-redis and swap-ssdb instance like this:
```
cd utils
# this will use the default "6379" port for swap-redis and "26379" port for swap-ssdb.
./deploy_redis.sh
# or you can specify a specific port like this, for example, use "6380" port
# ./deploy_redis.sh 6380

redis-cli -p 6379
127.0.0.1:6379> set a b
OK
127.0.0.1:6379> locatekey a
"redis"
127.0.0.1:6379> storetossdb a
OK
127.0.0.1:6379> locatekey a
"ssdb"
127.0.0.1:6379> get a
"b"
127.0.0.1:6379> dumpfromssdb a
OK
127.0.0.1:6379> locatekey a
"redis"
```

## Applicable scenes

* cache

SwapDB support LFU based heat statistics, hot keys are kept in redis，so you can use SwapDB as cache, which has the same performance as redis when access hot keys.

* High capacity redis-like KV storage

By configuring a low threshold of RAM/FLASH capacity, most of the data will be stored in disk and only the hottest data stored in redis.
