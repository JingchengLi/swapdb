start_server {tags {"ssdb"}} {
    r flushall
    test "Repopid basics" {
        assert_equal "repopid 0 0" [r repopid get] ;#repopid 0 0
        r repopid set 1 2
        r set foo bar12
        assert_equal "repopid 1 2" [r repopid get] ;#repopid 1 2
    }

    test "Repopid after flushall" {
        r flushall
        assert_equal "repopid 0 0" [r repopid get] ;#repopid 0 0
        r repopid set 2 3
        r set foo bar23
        assert_equal "repopid 2 3" [r repopid get] ;#repopid 2 3
    }

    test "Repopid not update if no real ops" {
        r repopid set 3 4
        assert_equal "repopid 2 3" [r repopid get] ;#repopid 2 3
    }

    test "Repopid update with real ops" {
        r set foo bar34
        assert_equal "repopid 3 4" [r repopid get] ;#repopid 3 4
    }

    test "Repopid abnormal update" {
        r repopid set 2 5 ;#receive timestamp earlier
        r set foo bar25
        puts [r repopid get] ;#????

        r repopid set 4 3 ;#receive id smaller
        r set foo bar43
        puts [r repopid get] ;#????

        r repopid set 5 6 ;#receive id skip from 4->6
        r set foo bar56
        puts [r repopid get] ;#????
    }

    test "Repopid slave->master(reset repopid) by two write ops" {
        r set foo bar
        assert_equal "repopid 0 0" [r repopid get] ;#repopid 0 0
    }

    test "Repopid master->slave" {
        r repopid set 1 2
        r set foo bar12
        assert_equal "repopid 1 2" [r repopid get] ;#repopid 1 2
    }

    # TODO get after set cannot reset repopid
    test "Repopid slave->master(reset repopid) by write/read ops" {
        r get foo
        assert_equal "repopid 0 0" [r repopid get] ;#repopid 0 0
    }

    test "Repopid no update with read ops" {
        r repopid set 1 2
        r get foo
        assert_equal "repopid 0 0" [r repopid get] ;#repopid 0 0
    }

    # TODO get will clear repopid set, so set after it will set no repopid
    test "Repopid reset with write ops after get" {
        r set foo bar12
        assert_equal "repopid 0 0" [r repopid get] ;#repopid 0 0
    }

    test "Repopid process receivedID < 0" {
        r repopid set 2 3
        r set foo bar23
        r repopid set 3 -1
        r set foo bar3_1
        assert_equal "repopid 2 3" [r repopid get] ;#repopid 2 3
    }

    test "Repopid can process args which is not available" {
        catch {r repopid ?} err
        assert_match "ERR*" $err
        catch {r repopid get 1} err
        puts "r repopid get 1:$err"
        catch {r repopid get a} err
        puts "r repopid get a:$err"
        catch {r repopid set 1} err
        puts "r repopid set 1:$err"
        catch {r repopid set 1 2 3} err
        puts "r repopid set 1 2 3:$err"
        catch {r repopid set a 2} err
        puts "r repopid set a 2:$err"
        catch {r repopid set 1 a} err
        puts "r repopid set 1 a:$err"

        puts "repopid get:[r repopid get]"
        r set foo bar
        puts "repopid get:[r repopid get]"
        catch {r repopid set 5 6} err
        puts "r repopid set 5 6:$err"
        puts "repopid get:[r repopid get]"
        r set foo bar56
        puts "repopid get:[r repopid get]"
    }

    test "Repopid expire" {
        r repopid set 6 7
        r setex foo 1 barexpire
        assert_equal "repopid 6 7" [r repopid get]
        r repopid set 7 8
        after 1100
        assert_equal "repopid 6 7" [r repopid get]
        r set foo bar
        assert_equal "repopid 7 8" [r repopid get]
    }

    test "Repopid update with String write ops not update with read ops" {
        r flushall
        r repopid set 0 1
        r set foo barnx nx
        assert_equal "repopid 0 1" [r repopid get] "r set foo barnx nx"
        r repopid set 1 2
        r set foo barxx xx
        assert_equal "repopid 1 2" [r repopid get] "r set foo barxx nx"

        r repopid set 2 3
        r get foo
        assert_equal "repopid 1 2" [r repopid get] "r get foo"
        r exists foo
        assert_equal "repopid 1 2" [r repopid get] "r exists foo"
        r expire foo 10
        assert_equal "repopid 2 3" [r repopid get] "r expire foo 10"

        r repopid set 3 4
        r persist foo
        assert_equal "repopid 3 4" [r repopid get] "r persist foo"

        r repopid set 4 5
        r pexpire foo 10
        assert_equal "repopid 4 5" [r repopid get] "r pexpire foo 10"

        r repopid set 5 6
        r expireat foo [expr [clock seconds]+15]
        assert_equal "repopid 5 6" [r repopid get] "r expireat foo [expr [clock seconds]+15]"

        r repopid set 6 7
        r pexpireat foo [expr ([clock seconds]*1000)+100]
        assert_equal "repopid 6 7" [r repopid get] "r pexpireat foo [expr ([clock seconds]*1000)+100]"

        r repopid set 7 8
        r del foo
        assert_equal "repopid 7 8" [r repopid get] "r del foo"

        r repopid set 8 9
        r setnx foo barnx
        assert_equal "repopid 8 9" [r repopid get] "r setnx foo barnx"

        r repopid set 9 10
        r setex foo 100 barex
        assert_equal "repopid 9 10" [r repopid get] "r setex foo 100 barex"

        r repopid set 10 11
        r psetex foo 1000000 bar
        assert_equal "repopid 10 11" [r repopid get] "r psetex foo 1000000 bar"

        r repopid set 11 12
        r type foo
        assert_equal "repopid 10 11" [r repopid get] "r type foo"
        r ttl foo
        assert_equal "repopid 10 11" [r repopid get] "r ttl foo"
        r pttl foo
        assert_equal "repopid 10 11" [r repopid get] "r pttl foo"
        r strlen foo
        assert_equal "repopid 10 11" [r repopid get] "r strlen foo"
        r getset foo 1
        assert_equal "repopid 11 12" [r repopid get] "r getset foo 1"

        r repopid set 12 13
        r decr foo
        assert_equal "repopid 12 13" [r repopid get] "r decr foo"

        r repopid set 13 14
        r decrby foo 2
        assert_equal "repopid 13 14" [r repopid get] "r decrby foo 2"

        r repopid set 14 15
        r incrby foo 2
        assert_equal "repopid 14 15" [r repopid get] "r incrby foo 2"

        r repopid set 15 16
        r incrbyfloat foo 1.1
        assert_equal "repopid 15 16" [r repopid get] "r incrbyfloat foo 1.1"

        r repopid set 16 17
        r incr foo
        assert_equal "repopid 16 17" [r repopid get] "r incr foo"

        r repopid set 17 18
        r append foo bar
        assert_equal "repopid 17 18" [r repopid get] "r append foo bar"

        r repopid set 18 19
        r substr foo 0 1
        assert_equal "repopid 17 18" [r repopid get] "r substr foo 0 1"
    }

    test "Repopid update with List write ops not update with read ops" {
        r flushall
        r repopid set 1 2
        r lpush mylist a b c d e
        assert_equal "repopid 1 2" [r repopid get] "r lpush mylist a b"

        r repopid set 2 3
        r rpush mylist a
        assert_equal "repopid 2 3" [r repopid get] "r rpush mylist a"

        r repopid set 3 4
        r lpop mylist
        assert_equal "repopid 3 4" [r repopid get] "r lpop mylist"

        r repopid set 4 5
        r rpop mylist
        assert_equal "repopid 4 5" [r repopid get] "r rpop mylist"

        r repopid set 5 6
        r llen mylist
        assert_equal "repopid 4 5" [r repopid get] "r llen mylist"
        r lrange mylist 0 -1
        assert_equal "repopid 4 5" [r repopid get] "r lrange mylist 0 -1"
        r ltrim mylist 0 1
        assert_equal "repopid 5 6" [r repopid get] "r ltrim mylist 0 -1"

        r repopid set 6 7
        r lindex mylist 0
        assert_equal "repopid 5 6" [r repopid get] "r lindex mylist 0"
        r rpushx mylist b
        assert_equal "repopid 6 7" [r repopid get] "r rpushx mylist b"

        r repopid set 7 8
        r lpushx mylist b
        assert_equal "repopid 7 8" [r repopid get] "r lpushx mylist b"

        r repopid set 8 9
        r lset mylist 0 c
        assert_equal "repopid 8 9" [r repopid get] "r lset mylist 0 c"
    }

    test "Repopid update with Hash write ops not update with read ops" {
        r flushall
        r repopid set 1 2
        r hmset myhash filed1 val1 filed2 val2 filed3 val3
        assert_equal "repopid 1 2" [r repopid get] "r hmset myhash filed1 val1 filed2 val2 filed3 val3"

        r repopid set 2 3
        r hdel myhash filed1
        assert_equal "repopid 2 3" [r repopid get] "r hdel myhash filed1"

        r repopid set 3 4
        r hgetall myhash
        assert_equal "repopid 2 3" [r repopid get] "r hgetall myhash"
        r hmget myhash field2 field3
        assert_equal "repopid 2 3" [r repopid get] "r hmget myhash field2 field3"
        r hget myhash field2
        assert_equal "repopid 2 3" [r repopid get] "r hget myhash field2"
        r hkeys myhash
        assert_equal "repopid 2 3" [r repopid get] "r hkeys myhash"
        r hlen myhash
        assert_equal "repopid 2 3" [r repopid get] "r hlen myhash"
        r hexists myhash field2
        assert_equal "repopid 2 3" [r repopid get] "r hexists myhash field2"

        r repopid set 4 5
        r hincrby myhash field 1
        assert_equal "repopid 4 5" [r repopid get] "r hincrby myhash field 1"

        r repopid set 5 6
        r hsetnx myhash field4 d
        assert_equal "repopid 5 6" [r repopid get] "r hsetnx myhash field4 d"

        r repopid set 6 7
        r hincrbyfloat myhash field 1.1
        assert_equal "repopid 6 7" [r repopid get] "r hincrbyfloat myhash field 1.1"

        r repopid set 7 8
        r hvals myhash
        assert_equal "repopid 6 7" [r repopid get] "r hvals myhash"
    }

    test "Repopid update with Set write ops not update with read ops" {
        r flushall
        r repopid set 1 2
        r sadd myset a b c d e f
        assert_equal "repopid 1 2" [r repopid get] "r sadd myset a b c d e f"

        r repopid set 2 3
        r smembers myset
        assert_equal "repopid 1 2" [r repopid get] "r smembers myset"
        r srem myset a
        assert_equal "repopid 2 3" [r repopid get] "r srem myset a"

        r repopid set 3 4
        r spop myset
        assert_equal "repopid 3 4" [r repopid get] "r spop myset"

        r repopid set 4 5
        r scard myset
        assert_equal "repopid 3 4" [r repopid get] "r scard myset"
        r sismember myset c
        assert_equal "repopid 3 4" [r repopid get] "r sismember myset c"
        r srandmember myset 2
        assert_equal "repopid 3 4" [r repopid get] "r srandmember myset 2"

        set Encoded [r dump myset]
        assert_equal "repopid 3 4" [r repopid get] "r dump myset"
        r restore myset 0 $Encoded replace 
        assert_equal "repopid 4 5" [r repopid get] "r restore myset 0 $Encoded replace "
    }

    test "Repopid update with Bit/Range write ops not update with read ops" {
        r flushall
        r repopid set 1 2
        r setbit foo 1 1
        assert_equal "repopid 1 2" [r repopid get] "r setbit foo 1 1"

        r repopid set 2 3
        r getbit foo 1
        assert_equal "repopid 1 2" [r repopid get] "r getbit foo 1"
        r bitcount foo
        assert_equal "repopid 1 2" [r repopid get] "r bitcount foo"
        r setrange foo 1 "Redis"
        assert_equal "repopid 2 3" [r repopid get] "r setrange foo 1 \"Redis\""

        r repopid set 3 4
        r getrange foo 1 2
        assert_equal "repopid 2 3" [r repopid get] "r getrange foo 1 2"
    }

    test "Repopid update with Zset write ops not update with read ops" {
        r flushall
        r repopid set 1 2
        r zadd foo 1 a 2 b 3 c 4 d 5 e 6 f
        assert_equal "repopid 1 2" [r repopid get] "r zadd foo 1 a 2 b 3 c 4 d 5 e 6 f"

        r repopid set 2 3
        r zrange foo 0 -1
        assert_equal "repopid 1 2" [r repopid get] "r zrange foo 0 -1"
        r zrem foo a
        assert_equal "repopid 2 3" [r repopid get] "r zrem foo a"

        r repopid set 3 4
        r zrank foo b
        assert_equal "repopid 2 3" [r repopid get] "r zrank foo b"
        r zrevrank foo b
        assert_equal "repopid 2 3" [r repopid get] "r zrevrank foo b"
        r zrevrange foo 0 -1
        assert_equal "repopid 2 3" [r repopid get] "r zrevrange foo 0 -1"
        r zcard foo
        assert_equal "repopid 2 3" [r repopid get] "r zcard foo"
        r zscore foo b
        assert_equal "repopid 2 3" [r repopid get] "r zscore foo b"
        r zcount foo -inf +inf
        assert_equal "repopid 2 3" [r repopid get] "r zcount foo -inf +inf"
        r zrangebyscore foo 2 3
        assert_equal "repopid 2 3" [r repopid get] "r zrangebyscore foo 2 3"
        r zrevrangebyscore foo 3 2
        assert_equal "repopid 2 3" [r repopid get] "r zrevrangebyscore foo 3 2"
        r zremrangebyrank foo 0 1
        assert_equal "repopid 3 4" [r repopid get] "r zremrangebyrank foo 0 1"

        r repopid set 4 5
        r zremrangebyscore foo 4 5
        assert_equal "repopid 4 5" [r repopid get] "r zremrangebyscore foo 4 5"

        r zadd foo 0 a 0 b 0 c 0 d 0 e 0 f
        r repopid set 5 6
        r zlexcount foo - +
        assert_equal "repopid 0 0" [r repopid get] "r zlexcount foo - +"
        r zrangebylex foo \[a \[c
        assert_equal "repopid 0 0" [r repopid get] "r zrangebylex foo \[a \[c"
        r zrevrangebylex foo \[c \[a
        assert_equal "repopid 0 0" [r repopid get] "r zrevrangebylex foo \[c \[a"
        assert_equal {PONG} [r ping]
        assert_equal "repopid 0 0" [r repopid get]
    }
}
