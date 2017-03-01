start_server {tags {"ssdb"}} {
    test {basic commands - keys} {
        ssdbr del key
        ssdbr set key bar
        assert_equal [ssdbr get key] {bar} "get key"
        assert_equal [ssdbr exists key] 1 "exists key"
        assert_equal [ssdbr expire key 100] 1 "expire key"
        assert_equal [ssdbr pexpire key 100000] 1 "pexpire key"
        assert_equal [ssdbr persist key] 1 "persist key"

        assert_equal [ssdbr type key] {string} "TYPE mykey"

        set time [expr {[clock seconds]+15}]
        assert_equal [ssdbr EXPIREAT key $time] 1 "EXPIREAT key"
        assert { [ssdbr TTL key] > 13 }
        set time [expr {[clock seconds]*1000+10000}]
        assert_equal [ssdbr PEXPIREAT key $time] 1 "PEXPIREAT key"
        assert { [ssdbr PTTL key] > 8000 }
        assert_equal [ssdbr del key] 1 "del key"
    }

    test {basic commands - strings} {
        ssdbr del mykey
        assert_equal [ssdbr set mykey 5] {OK} "set mykey"
        assert_equal [ssdbr get mykey] 5 "get mykey"
        assert_equal [ssdbr decr mykey] 4 "decr mykey"
        assert_equal [ssdbr incr mykey] 5 "incr mykey"
        assert_equal [ssdbr decrby mykey 3] 2 "decrby mykey"
        assert_equal [ssdbr incrby mykey 3] 5 "incrby mykey"
        assert_equal [ssdbr incrbyfloat mykey 0.5] 5.5 "incrbyfloat mykey"

        ssdbr del mykey
        assert_equal [ssdbr SETNX mykey "Hello"] 1 "SETNX mykey"
        assert_equal [ssdbr SETNX mykey "Hello"] 0 "SETNX mykey"
        assert_equal [ssdbr setex mykey 10 "Hello"] {OK} "setex mykey"
        assert_equal [ssdbr psetex mykey 1000 "Hello"] {OK} "PSETNX mykey"
        assert { [ssdbr PTTL mykey] > 900 }

        assert_equal [ssdbr strlen mykey] 5 "strlen mykey"
        assert_equal [ssdbr getset mykey world] {Hello} "getset mykey"
        assert_equal [ssdbr get mykey] {world} "getset mykey"

        assert_equal [ssdbr append mykey world] 10 "append mykey"
        assert_equal [ssdbr setrange mykey 0 hello] 10 "setrange mykey"
        assert_equal [ssdbr substr mykey 0 4] {hello} "substr mykey"
        assert_equal [ssdbr getrange mykey 5 9] {world} "getrange mykey"

        ssdbr del mykey
        assert_equal [ssdbr setbit mykey 7 1] 0 "setbit mykey"
        assert_equal [ssdbr getbit mykey 0] 0 "getbit mykey"
        assert_equal [ssdbr getbit mykey 7] 1 "getbit mykey"
        assert_equal [ssdbr setbit mykey 7 1] 1 "setbit mykey"
        assert_equal [ssdbr bitcount mykey] 1 "bitcount mykey"

        assert_equal [ssdbr setbit mykey 6 1] 0 "setbit mykey"
        assert_equal [ssdbr bitcount mykey] 2 "bitcount mykey"
        assert_equal [ssdbr del mykey] 1 "del mykey"
    }

    test {basic commands - list} {
        ssdbr del mylist
        assert_equal [ssdbr rpushx mylist "world"] 0 "rpushx mylist"
        assert_equal [ssdbr lpushx mylist "hello"] 0 "lpushx mylist"

        assert_equal [ssdbr rpush mylist "world"] 1 "rpush mylist"
        assert_equal [ssdbr rpushx mylist "world"] 2 "rpushx mylist"
        assert_equal [ssdbr rpop mylist] "world" "rpop mylist"
        assert_equal [ssdbr lpushx mylist "world"] 2 "lpushx mylist"
        assert_equal [ssdbr lpop mylist] "world" "lpop mylist"

        assert_equal [ssdbr lset mylist 0 "world"] {OK} "lset mylist"

        assert_equal [ssdbr lpush mylist "hello"] 2 "lpush mylist"
        assert_equal [ssdbr lrange mylist 0 -1] {hello world} "lrange mylist"
        assert_equal [ssdbr llen mylist] 2 "llen mylist"
        assert_equal [ssdbr lindex mylist 0] "hello" "lindex mylist"
        assert_equal [ssdbr rpop mylist] "world" "rpop mylist"
        assert_equal [ssdbr lpop mylist] "hello" "lpop mylist"
        assert_equal [ssdbr del mylist] 0 "del mylist"
    }

    test {basic commands - hash} {
        ssdbr del myhash
        assert_equal [ssdbr HSET myhash field1 foo] 1 "hset myhash"
        assert_equal [ssdbr HSET myhash field2 bar] 1 "hset myhash"
        assert_equal [ssdbr HGETALL myhash] {field1 foo field2 bar} "hgetall myhash"
        assert_equal [ssdbr HDEl myhash field1 field2] 2 "hdel myhash"

        assert_equal [ssdbr HMSET myhash field1 foo field2 bar] {OK} "hmset myhash"
        assert_equal [ssdbr HKEYS myhash] {field1 field2} "hkeys myhash"
        assert_equal [ssdbr Hvals myhash] {foo bar} "hvals myhash"
        assert_equal [ssdbr Hlen myhash] 2 "hlen myhash"
        assert_equal [ssdbr hexists myhash field1] 1 "hexists myhash"

        assert_equal [ssdbr HSETNX myhash field1 1] 0 "hsetnx myhash"
        assert_equal [ssdbr HGET myhash field1] {foo} "hget myhash"
        assert_equal [ssdbr HSETNX myhash field3 1] 1 "hsetnx myhash"
        assert_equal [ssdbr HINCRBY myhash field3 2] 3 "hincrby myhash"
        #for hincrby will incr hlen issue
        assert_equal [ssdbr Hlen myhash] 3 "hlen myhash"
        assert_equal [ssdbr HINCRBYFLOAT myhash field3 0.5] 3.5 "hincrbyfloat myhash"
        assert_equal [ssdbr Hlen myhash] 3 "hlen myhash"
        assert_equal [ssdbr del myhash] 1 "del myhash"
    }

    test {basic commands - set} {
        ssdbr del myset
        assert_equal [ssdbr sadd myset "hello"] 1 "sadd myset hello"
        assert_equal [ssdbr sadd myset "world"] 1 "sadd myset world"
        assert_equal [ssdbr sadd myset "world"] 0 "sadd myset world twice"
        assert_equal [ssdbr scard myset] 2 "scard myset"
        assert_equal [ssdbr sismember myset hello] 1 "sismember myset"
        assert_equal [ssdbr smembers myset] {hello world} "smembers myset"

        assert_equal [ssdbr srem myset "hello"] 1 "srem myset"
        assert_equal [ssdbr srandmember myset] world "srandmember myset"
        assert_equal [ssdbr spop myset] {world} "spop myset"
        assert_equal [ssdbr del myset] 0 "del myset"
    }

    test {basic commands - dump/restore} {
        ssdbr del mykey
        ssdbr set mykey 10
        set dumpkey [ssdbr dump mykey]
        ssdbr del mykey
        ssdbr restore mykey 0 $dumpkey
        assert_equal [ssdbr get mykey] 10 "dump/restore"
        assert_equal [ssdbr del mykey] 1 "del mykey"
    }

    test {basic commands - zset} {
        ssdbr del myzset
        assert_equal [ssdbr zadd myzset 1 "one"] 1 "zadd myzset one"
        assert_equal [ssdbr zadd myzset 2 "two"] 1 "zadd myzset two"
        assert_equal [ssdbr zrank myzset "two"] 1 "zrank myzset"
        assert_equal [ssdbr zrevrank myzset "one"] 1 "zrevrank myzset"
        assert_equal [ssdbr zcard myzset] 2 "zcard myzset"
        assert_equal [ssdbr zcount myzset 1 2] 2 "zcount myzset"
        assert_equal [ssdbr zrangebyscore myzset 1 2] {one two} "zrangebyscore myzset"

        assert_equal [ssdbr zremrangebyrank myzset 0 0] 1 "zremrangebyrank myzset"
        assert_equal [ssdbr zadd myzset 1 "one"] 1 "zadd myzset"
        assert_equal [ssdbr zremrangebyscore myzset 1 1] 1 "zremrangebyscore myzset"
        assert_equal [ssdbr zadd myzset 2 "two"] 1 "zadd myzset"


        assert_equal [ssdbr ZRANGE myzset 0 -1 WITHSCORES] {one 1 two 2} "zrange myzset"
        assert_equal [ssdbr ZREVRANGE myzset 0 -1 WITHSCORES] {two 2 one 1} "zrevrange myzset"
        assert_equal [ssdbr zrem myzset "two"] 1 "zrem myzset"

        assert_equal [ssdbr zincrby myzset 2 "one"] 3 "zincryby myzset"
        assert_equal [ssdbr zscore myzset "one"] 3 "zscore myzset"
        assert_equal [ssdbr del myzset] 0 "del myzset"
    }
}
