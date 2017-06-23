start_server {tags {"ssdb"}} {
    proc restart_ssdb {} {
        catch { exec ../../../../ssdb-server -d ../tmp/ssdb_8888.config -s restart } err
        set srv [lindex $::servers 0]
        set retry 50
        while {$retry} {
            catch {redis $::host $::port} err
            if {![string match {*connection refused*} $err]} {
                dict set srv "client" $err
                break
            }
            after 100
            incr retry -1
        }
        if {0 == $retry} {
            error "ssdb server not restart after 5s."
        }
        lset ::servers 0 $srv
    }

    set time [clock seconds]
    set mstime [clock milliseconds]
    foreach timestamp [list 0 $time $mstime] {
        test "no crash ssdb at first" {
            r repopid set 0 0
            assert_equal "OK" [r set foo bar]
            # assert_error "I/O error reading reply" {r set foo bar}
        }

        test "Repopid basics" {
            # restart_ssdb

            r repopid set [expr $timestamp+1] 1
            r set foo bar
            assert_equal "repopid [expr $timestamp+1] 1" [r repopid get] ;#repopid 1 1
            r repopid set [expr $timestamp+1] 2
            r set foo bar01
            assert_equal "repopid [expr $timestamp+1] 2" [r repopid get] ;#repopid 1 2

            r repopid set [expr $timestamp+2] 1
            r set foo bar21
            assert_equal "repopid [expr $timestamp+2] 1" [r repopid get] ;#repopid 2 1

            r repopid set [expr $timestamp+2] 2
            r set foo bar22
            assert_equal "repopid [expr $timestamp+2] 2" [r repopid get] ;#repopid 2 2
        }

        test "Repopid with flushall(just a write ops)" {
            r repopid set [expr $timestamp+2] 3
            r flushall ;#no reset
            assert_equal "repopid [expr $timestamp+2] 3" [r repopid get] ;#repopid 2 3
        }

        test "Repopid after flushall" {
            r repopid set [expr $timestamp+2] 4
            r set foo bar24
            assert_equal "repopid [expr $timestamp+2] 4" [r repopid get] ;#repopid 2 4
        }

        test "Repopid not update if no real ops" {
            r repopid set [expr $timestamp+3] 1
            r set foo bar31
            r repopid set [expr $timestamp+3] 2
            assert_equal "repopid [expr $timestamp+3] 1" [r repopid get] ;#repopid 3 1
        }

        test "Repopid receive timestamp earlier" {
            r repopid set [expr $timestamp+2] 5 ;#receive timestamp earlier
            assert_equal "OK" [r set foo bar25]
            # assert_error "I/O error reading reply" {r set foo bar25}
        }

        test "Repopid receive id smaller" {
            # restart_ssdb
            r repopid set [expr $timestamp+1] 1
            r set foo bar
            r repopid set [expr $timestamp+1] 2
            r set foo bar12
            r repopid set [expr $timestamp+1] 3
            r set foo bar13
            assert_equal "repopid [expr $timestamp+1] 3" [r repopid get] ;#repopid 1 3
            r repopid set [expr $timestamp+1] 1 ;#receive id smaller
            assert_equal "OK" [r set foo bar]
            # assert_error "I/O error reading reply" {r set foo bar}
        }

        test "Repopid receive id skip from 2->4" {
            restart_ssdb
            r repopid set [expr $timestamp+1] 1
            r set foo bar
            r repopid set [expr $timestamp+1] 2
            r set foo bar11
            r repopid set [expr $timestamp+1] 4
            # policy update, ssdb not assert 2->4.
            r set foo bar
            assert_equal "repopid [expr $timestamp+1] 4" [r repopid get]
            # assert_error "I/O error reading reply" {r set foo bar}
        }

        test "Repopid receive id not 1 and timestamp changed" {
            restart_ssdb
            r repopid set [expr $timestamp+1] 1
            r set foo bar
            r repopid set [expr $timestamp+1] 2
            r set foo bar11
            r repopid set [expr $timestamp+2] 2
            r set foo bar
            assert_equal "repopid [expr $timestamp+2] 2" [r repopid get]
            # assert_error "I/O error reading reply" {r set foo bar}
        }

        # remove it.
        #test "ReplLink not receive two write ops continous" {
        #    restart_ssdb
        #    r repopid set [expr $timestamp+1] 1
        #    r set foo bar
        #    r repopid set [expr $timestamp+1] 2 ;# recognize is repllink when receive repopid
        #    r set foo12 bar12
        #    assert_error "I/O error reading reply" {r set foo bar}
        #}

        # TODO 实际中ReplLink不会收到读操作，当前ssdb对这种异常情况不做任何处理，应该是可以的。
        #    test "ReplLink not receive read ops" {
        #        restart_ssdb
        #        r get foo
        #        r repopid set [expr $timestamp+1] 1
        #        assert_error "I/O error reading reply" {r get foo}
        #    }

        # ssdb not assert it, redis assert it.
        #test "Repopid process receivedID < 0" {
        #    restart_ssdb
        #    r repopid set [expr $timestamp+1] 1
        #    r set foo bar
        #    r repopid set [expr $timestamp+3] -1
        #    assert_error "I/O error reading reply" {r set foo bar}
        #}

        test "Repopid process receivedID skip at first 1->3" {
            restart_ssdb
            r repopid set [expr $timestamp+1] 1
            r set foo bar
            r repopid set [expr $timestamp+1] 3
            r set foo bar
            assert_equal "repopid [expr $timestamp+1] 3" [r repopid get]
            # assert_error "I/O error reading reply" {r set foo bar}
        }

        test "Repopid can process args which is not available" {
            restart_ssdb
            r repopid set [expr $timestamp+1] 1
            r set foo bar
            assert_error "ERR*" {r repopid ?}
            catch {r repopid get 1} err
            puts "r repopid get 1:$err"
            catch {r repopid get a} err
            puts "r repopid get a:$err"
            assert_error "ERR*" {r repopid set [expr $timestamp+1]}
            catch {r repopid set [expr $timestamp+1] 3 4} err
            puts "r repopid set [expr $timestamp+1] 3 4:$err"
            assert_error "ERR*" {r repopid set a 2}
            assert_error "ERR*" {r repopid set [expr $timestamp+1] a}

            r repopid set [expr $timestamp+1] 2
            r set foo bar
            assert_equal "repopid [expr $timestamp+1] 2" [r repopid get]
        }

        test "Repopid expire" {
            r repopid set [expr $timestamp+5] 1
            r setex foo 1 barexpire
            assert_equal "repopid [expr $timestamp+5] 1" [r repopid get]
            r repopid set [expr $timestamp+5] 2
            after 1100
            assert_equal "repopid [expr $timestamp+5] 1" [r repopid get]
            r set foo bar
            assert_equal "repopid [expr $timestamp+5] 2" [r repopid get]
        }

        test "Repopid update with String write ops not update with read ops" {
            r repopid set [expr $timestamp+6] 1
            r set foo barnx nx
            assert_equal "repopid [expr $timestamp+6] 1" [r repopid get] "r set foo barnx nx"
            r repopid set [expr $timestamp+6] 2
            r set foo barxx xx
            assert_equal "repopid [expr $timestamp+6] 2" [r repopid get] "r set foo barxx nx"

            r repopid set [expr $timestamp+6] 3
            r get foo
            assert_equal "repopid [expr $timestamp+6] 2" [r repopid get] "r get foo"
            r exists foo
            assert_equal "repopid [expr $timestamp+6] 2" [r repopid get] "r exists foo"
            if {$::expire} {
                r expire foo 10
                assert_equal "repopid [expr $timestamp+6] 3" [r repopid get] "r expire foo 10"
            }

            if {$::expire} {
                r repopid set [expr $timestamp+6] 4
                r persist foo
                assert_equal "repopid [expr $timestamp+6] 4" [r repopid get] "r persist foo"

                r repopid set [expr $timestamp+6] 5
                r pexpire foo 10
                assert_equal "repopid [expr $timestamp+6] 5" [r repopid get] "r pexpire foo 10"

                r repopid set [expr $timestamp+6] 6
                r expireat foo [expr [clock seconds]+15]
                assert_equal "repopid [expr $timestamp+6] 6" [r repopid get] "r expireat foo [expr [clock seconds]+15]"

                r repopid set [expr $timestamp+6] 7
                r pexpireat foo [expr ([clock seconds]*1000)+100]
                assert_equal "repopid [expr $timestamp+6] 7" [r repopid get] "r pexpireat foo [expr ([clock seconds]*1000)+100]"
            }

            r repopid set [expr $timestamp+6] 8
            r del foo
            assert_equal "repopid [expr $timestamp+6] 8" [r repopid get] "r del foo"

            r repopid set [expr $timestamp+6] 9
            r setnx foo barnx
            assert_equal "repopid [expr $timestamp+6] 9" [r repopid get] "r setnx foo barnx"

            if {$::expire} {
                r repopid set [expr $timestamp+6] 10
                r setex foo 100 barex
                assert_equal "repopid [expr $timestamp+6] 10" [r repopid get] "r setex foo 100 barex"

                r repopid set [expr $timestamp+6] 11
                r psetex foo 1000000 bar
                assert_equal "repopid [expr $timestamp+6] 11" [r repopid get] "r psetex foo 1000000 bar"
            }

            r repopid set [expr $timestamp+6] 12
            if {$::expire} {
                r ttl foo
                assert_equal "repopid [expr $timestamp+6] 11" [r repopid get] "r ttl foo"
                r pttl foo
                assert_equal "repopid [expr $timestamp+6] 11" [r repopid get] "r pttl foo"
            }

            r getset foo 1
            assert_equal "repopid [expr $timestamp+6] 12" [r repopid get] "r getset foo 1"

            r repopid set [expr $timestamp+6] 13
            r strlen foo
            assert_equal "repopid [expr $timestamp+6] 12" [r repopid get] "r strlen foo"
            r type foo
            assert_equal "repopid [expr $timestamp+6] 12" [r repopid get] "r type foo"

            r decr foo
            assert_equal "repopid [expr $timestamp+6] 13" [r repopid get] "r decr foo"

            r repopid set [expr $timestamp+6] 14
            r decrby foo 2
            assert_equal "repopid [expr $timestamp+6] 14" [r repopid get] "r decrby foo 2"

            r repopid set [expr $timestamp+6] 15
            r incrby foo 2
            assert_equal "repopid [expr $timestamp+6] 15" [r repopid get] "r incrby foo 2"

            r repopid set [expr $timestamp+6] 16
            r incr foo
            assert_equal "repopid [expr $timestamp+6] 16" [r repopid get] "r incr foo"

            r repopid set [expr $timestamp+6] 17
            r incrbyfloat foo 1.1
            assert_equal "repopid [expr $timestamp+6] 17" [r repopid get] "r incrbyfloat foo 1.1"

            r repopid set [expr $timestamp+6] 18
            r append foo bar
            assert_equal "repopid [expr $timestamp+6] 18" [r repopid get] "r append foo bar"

            r repopid set [expr $timestamp+6] 19
            r substr foo 0 1
            assert_equal "repopid [expr $timestamp+6] 18" [r repopid get] "r substr foo 0 1"
        }

        test "Repopid update with List write ops not update with read ops" {
            r repopid set [expr $timestamp+6] 19
            r lpush mylist a b c d e
            assert_equal "repopid [expr $timestamp+6] 19" [r repopid get] "r lpush mylist a b"

            r repopid set [expr $timestamp+6] 20
            r rpush mylist a
            assert_equal "repopid [expr $timestamp+6] 20" [r repopid get] "r rpush mylist a"

            r repopid set [expr $timestamp+7] 1
            r lpop mylist
            assert_equal "repopid [expr $timestamp+7] 1" [r repopid get] "r lpop mylist"

            r repopid set [expr $timestamp+8] 1
            r rpop mylist
            assert_equal "repopid [expr $timestamp+8] 1" [r repopid get] "r rpop mylist"

            r repopid set [expr $timestamp+9] 1
            r llen mylist
            assert_equal "repopid [expr $timestamp+8] 1" [r repopid get] "r llen mylist"
            r lrange mylist 0 -1
            assert_equal "repopid [expr $timestamp+8] 1" [r repopid get] "r lrange mylist 0 -1"
            r ltrim mylist 0 1
            assert_equal "repopid [expr $timestamp+9] 1" [r repopid get] "r ltrim mylist 0 -1"

            r repopid set [expr $timestamp+10] 1
            r lindex mylist 0
            assert_equal "repopid [expr $timestamp+9] 1" [r repopid get] "r lindex mylist 0"
            r rpushx mylist b
            assert_equal "repopid [expr $timestamp+10] 1" [r repopid get] "r rpushx mylist b"

            r repopid set [expr $timestamp+11] 1
            r lpushx mylist b
            assert_equal "repopid [expr $timestamp+11] 1" [r repopid get] "r lpushx mylist b"

            r repopid set [expr $timestamp+12] 1
            r lset mylist 0 c
            assert_equal "repopid [expr $timestamp+12] 1" [r repopid get] "r lset mylist 0 c"
        }

        test "Repopid update with Hash write ops not update with read ops" {
            r repopid set [expr $timestamp+13] 1
            r hmset myhash filed1 val1 filed2 val2 filed3 val3
            assert_equal "repopid [expr $timestamp+13] 1" [r repopid get] "r hmset myhash filed1 val1 filed2 val2 filed3 val3"

            r repopid set [expr $timestamp+15] 1
            r hdel myhash filed1
            assert_equal "repopid [expr $timestamp+15] 1" [r repopid get] "r hdel myhash filed1"

            r repopid set [expr $timestamp+17] 1
            r hgetall myhash
            assert_equal "repopid [expr $timestamp+15] 1" [r repopid get] "r hgetall myhash"
            r hmget myhash field2 field3
            assert_equal "repopid [expr $timestamp+15] 1" [r repopid get] "r hmget myhash field2 field3"
            r hget myhash field2
            assert_equal "repopid [expr $timestamp+15] 1" [r repopid get] "r hget myhash field2"
            r hkeys myhash
            assert_equal "repopid [expr $timestamp+15] 1" [r repopid get] "r hkeys myhash"
            r hlen myhash
            assert_equal "repopid [expr $timestamp+15] 1" [r repopid get] "r hlen myhash"
            r hexists myhash field2
            assert_equal "repopid [expr $timestamp+15] 1" [r repopid get] "r hexists myhash field2"
            r hincrby myhash field 1
            assert_equal "repopid [expr $timestamp+17] 1" [r repopid get] "r hincrby myhash field 1"

            r repopid set [expr $timestamp+19] 1
            r hsetnx myhash field4 d
            assert_equal "repopid [expr $timestamp+19] 1" [r repopid get] "r hsetnx myhash field4 d"

            r repopid set [expr $timestamp+21] 1
            r hincrbyfloat myhash field 1.1
            assert_equal "repopid [expr $timestamp+21] 1" [r repopid get] "r hincrbyfloat myhash field 1.1"

            r repopid set [expr $timestamp+23] 1
            r hvals myhash
            assert_equal "repopid [expr $timestamp+21] 1" [r repopid get] "r hvals myhash"
        }

        test "Repopid update with Set write ops not update with read ops" {
            r repopid set [expr $timestamp+25] 1
            r sadd myset a b c d e f
            assert_equal "repopid [expr $timestamp+25] 1" [r repopid get] "r sadd myset a b c d e f"

            r repopid set [expr $timestamp+25] 2
            r smembers myset
            assert_equal "repopid [expr $timestamp+25] 1" [r repopid get] "r smembers myset"
            r srem myset a
            assert_equal "repopid [expr $timestamp+25] 2" [r repopid get] "r srem myset a"

            r repopid set [expr $timestamp+25] 3
            r spop myset
            assert_equal "repopid [expr $timestamp+25] 3" [r repopid get] "r spop myset"

            r repopid set [expr $timestamp+29] 1
            r scard myset
            assert_equal "repopid [expr $timestamp+25] 3" [r repopid get] "r scard myset"
            r sismember myset c
            assert_equal "repopid [expr $timestamp+25] 3" [r repopid get] "r sismember myset c"
            r srandmember myset 2
            assert_equal "repopid [expr $timestamp+25] 3" [r repopid get] "r srandmember myset 2"

            set Encoded [r dump myset]
            assert_equal "repopid [expr $timestamp+25] 3" [r repopid get] "r dump myset"
            r restore myset 0 $Encoded replace 
            assert_equal "repopid [expr $timestamp+29] 1" [r repopid get] "r restore myset 0 $Encoded replace "
        }

        test "Repopid update with Bit/Range write ops not update with read ops" {
            r repopid set [expr $timestamp+29] 2
            r setbit foo 1 1
            assert_equal "repopid [expr $timestamp+29] 2" [r repopid get] "r setbit foo 1 1"

            r repopid set [expr $timestamp+29] 3
            r getbit foo 1
            assert_equal "repopid [expr $timestamp+29] 2" [r repopid get] "r getbit foo 1"
            r bitcount foo
            assert_equal "repopid [expr $timestamp+29] 2" [r repopid get] "r bitcount foo"
            r setrange foo 1 "Redis"
            assert_equal "repopid [expr $timestamp+29] 3" [r repopid get] "r setrange foo 1 \"Redis\""

            r repopid set [expr $timestamp+29] 4
            r getrange foo 1 2
            assert_equal "repopid [expr $timestamp+29] 3" [r repopid get] "r getrange foo 1 2"
        }

        test "Repopid update with Zset write ops not update with read ops" {
            r repopid set [expr $timestamp+30] 1
            r zadd myzset 1 a 2 b 3 c 4 d 5 e 6 f
            assert_equal "repopid [expr $timestamp+30] 1" [r repopid get] "r zadd myzset 1 a 2 b 3 c 4 d 5 e 6 f"

            r repopid set [expr $timestamp+30] 2
            r zrange myzset 0 -1
            assert_equal "repopid [expr $timestamp+30] 1" [r repopid get] "r zrange myzset 0 -1"
            r zrem myzset a
            assert_equal "repopid [expr $timestamp+30] 2" [r repopid get] "r zrem myzset a"

            r repopid set [expr $timestamp+30] 3
            r zrank myzset b
            assert_equal "repopid [expr $timestamp+30] 2" [r repopid get] "r zrank myzset b"
            r zrevrank myzset b
            assert_equal "repopid [expr $timestamp+30] 2" [r repopid get] "r zrevrank myzset b"
            r zrevrange myzset 0 -1
            assert_equal "repopid [expr $timestamp+30] 2" [r repopid get] "r zrevrange myzset 0 -1"
            r zcard myzset
            assert_equal "repopid [expr $timestamp+30] 2" [r repopid get] "r zcard myzset"
            r zscore myzset b
            assert_equal "repopid [expr $timestamp+30] 2" [r repopid get] "r zscore myzset b"
            r zcount myzset -inf +inf
            assert_equal "repopid [expr $timestamp+30] 2" [r repopid get] "r zcount myzset -inf +inf"
            r zrangebyscore myzset 2 3
            assert_equal "repopid [expr $timestamp+30] 2" [r repopid get] "r zrangebyscore myzset 2 3"
            r zrevrangebyscore myzset 3 2
            assert_equal "repopid [expr $timestamp+30] 2" [r repopid get] "r zrevrangebyscore myzset 3 2"
            r zremrangebyrank myzset 0 1
            assert_equal "repopid [expr $timestamp+30] 3" [r repopid get] "r zremrangebyrank myzset 0 1"

            r repopid set [expr $timestamp+100] 1
            r zremrangebyscore myzset 4 5
            assert_equal "repopid [expr $timestamp+100] 1" [r repopid get] "r zremrangebyscore myzset 4 5"

            r repopid set [expr $timestamp+100] 2
            r zadd myzset 0 a 0 b 0 c 0 d 0 e 0 f
            r repopid set [expr $timestamp+100] 3
            r zlexcount myzset - +
            assert_equal "repopid [expr $timestamp+100] 2" [r repopid get] "r zlexcount myzset - +"
            r zrangebylex myzset \[a \[c
            assert_equal "repopid [expr $timestamp+100] 2" [r repopid get] "r zrangebylex myzset \[a \[c"
            r zrevrangebylex myzset \[c \[a
            assert_equal "repopid [expr $timestamp+100] 2" [r repopid get] "r zrevrangebylex myzset \[c \[a"
            assert_equal {PONG} [r ping]
            assert_equal "repopid [expr $timestamp+100] 2" [r repopid get]
        }

        test "Repopid update with del" {
            r repopid set [expr $timestamp+101] 1
            r del foo11
            assert_equal "repopid [expr $timestamp+101] 1" [r repopid get]
            r repopid set [expr $timestamp+101] 2
            r del foo myhash mylist myset myzset
            assert_equal "repopid [expr $timestamp+101] 2" [r repopid get]
        }

        test "Repopid update even with no keys changes" {
            r repopid set [expr $timestamp+102] 1
            r del foo11
            assert_equal "repopid [expr $timestamp+102] 1" [r repopid get]
            r repopid set [expr $timestamp+102] 2
            r del foo myhash mylist myset myzset
            assert_equal "repopid [expr $timestamp+102] 2" [r repopid get]
            r repopid set [expr $timestamp+102] 3
            r set foo bar
            assert_equal "repopid [expr $timestamp+102] 3" [r repopid get]
            r repopid set [expr $timestamp+102] 4
            r set foo bar
            assert_equal "repopid [expr $timestamp+102] 4" [r repopid get]
            r repopid set [expr $timestamp+102] 5
            r del foo
            assert_equal "repopid [expr $timestamp+102] 5" [r repopid get]
        }

        test "Flushall clean" {
            restart_ssdb
            r flushall
            r ping
        } {PONG}
    }
}
