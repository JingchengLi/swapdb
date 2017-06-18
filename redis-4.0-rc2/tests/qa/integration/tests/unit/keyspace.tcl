start_server {tags {"keyspace"}} {
    test {DEL against a single item} {
        ssdbr set x foo
        assert {[ssdbr get x] eq "foo"}
        ssdbr del x
        ssdbr get x
    } {}

#    test {Vararg DEL} {
#        r set foo1 a
#        r set foo2 b
#        r set foo3 c
#        list [ssdbr del foo1 foo2 foo3 foo4] [ssdbr mget foo1 foo2 foo3]
#    } {3 {{} {} {}}}

    test {KEYS with pattern} {
        foreach key {key_x key_y key_z foo_a foo_b foo_c} {
            ssdbr set $key hello
        }
        lsort [ssdbr keys foo*]
    } {foo_a foo_b foo_c}

    test {KEYS to get all keys} {
        lsort [ssdbr keys *]
    } {foo_a foo_b foo_c key_x key_y key_z}

    test {DBSIZE} {
        ssdbr dbsize
    } {6}

#    test {DEL all keys} {
#        foreach key [ssdbr keys *] {r del $key}
#        r dbsize
#    } {0}

    test "DEL against expired key" {
        ssdbr debug set-active-expire 0
        ssdbr setex keyExpire 1 valExpire
        after 1100
        assert_equal 0 [r del keyExpire]
        ssdbr debug set-active-expire 1
    }

    test {EXISTS} {
        set res {}
        ssdbr set newkey test
        append res [ssdbr exists newkey]
        ssdbr del newkey
        append res [ssdbr exists newkey]
    } {10}

    test {Zero length value in key. SET/GET/EXISTS} {
        ssdbr set emptykey {}
        set res [ssdbr get emptykey]
        append res [ssdbr exists emptykey]
        ssdbr del emptykey
        append res [ssdbr exists emptykey]
    } {10}

    test {Commands pipelining} {
        set fd [r channel]
        puts -nonewline $fd "SET k1 xyzk\r\nGET k1\r\nPING\r\n"
        flush $fd
        set res {}
        append res [string match OK* [r read]]
        append res [r read]
        append res [string match PONG* [r read]]
        format $res
    } {1xyzk1}

    test {Non existing command} {
        catch {r foobaredcommand} err
        string match ERR* $err
    } {1}

    test {DEL all keys again (DB 0)} {
        foreach key [ssdbr keys *] {
            ssdbr del $key
        }
        ssdbr dbsize
    } {0}

    test {DEL all keys again (DB 1)} {
        ssdbr select 10
        foreach key [ssdbr keys *] {
            ssdbr del $key
        }
        set res [ssdbr dbsize]
        ssdbr select 9
        format $res
    } {0}

#    test {SET/GET keys in different DBs} {
#        ssdbr set a hello
#        ssdbr set b world
#        ssdbr select 10
#        ssdbr set a foo
#        ssdbr set b bared
#        ssdbr select 9
#        set res {}
#        lappend res [ssdbr get a]
#        lappend res [ssdbr get b]
#        ssdbr select 10
#        lappend res [ssdbr get a]
#        lappend res [ssdbr get b]
#        ssdbr select 9
#        format $res
#    } {hello world foo bared}

    test {KEYS * two times with long key, Github issue #1208} {
        ssdbr flushdb
        ssdbr set dlskeriewrioeuwqoirueioqwrueoqwrueqw test
        ssdbr keys *
        ssdbr keys *
    } {dlskeriewrioeuwqoirueioqwrueoqwrueqw}
}
