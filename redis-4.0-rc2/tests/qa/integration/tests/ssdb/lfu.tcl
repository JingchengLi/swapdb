start_server {tags {"lfu"}} {
    test "lfu-log-factor config" {
        set factor 1
        assert_equal {OK} [r config set lfu-log-factor $factor] "set lfu-log-factor"
        assert_equal $factor [lindex [r config get lfu-log-factor] 1] "get lfu-log-factor"
    }

    test "lfu-decay-time config" {
        set decaytime 2
        assert_equal {OK} [r config set lfu-decay-time $factor] "set lfu-decay-time"
        assert_equal $factor [lindex [r config get lfu-decay-time] 1] "get lfu-decay-time"
    }

    test "ssdb-transfer-lower-limit config" {
        set lower 20
        assert_equal {OK} [r config set ssdb-transfer-lower-limit $factor] "set ssdb-transfer-lower-limit"
        assert_equal $factor [lindex [r config get ssdb-transfer-lower-limit] 1] "get ssdb-transfer-lower-limit"
    }

    test "ssdb-load-upper-limit config" {
        set decaylimit 50
        assert_equal {OK} [r config set ssdb-load-upper-limit $factor] "set ssdb-load-upper-limit"
        assert_equal $factor [lindex [r config get ssdb-load-upper-limit] 1] "get ssdb-load-upper-limit"
    }
}

start_server {tags {"lfu"}} {
    set maxhit 10001
    for {set i 1} {$i < $maxhit} {incr i} {
        r incr foo
        if {100 == $i || 1000 == $i || 10000 == $i || 100000 == $i || 1000000 == $i} {
            puts "$i hits:[r object freq foo]"
        }
    }
}

start_server {tags {"lfu"}
overrides {maxmemory 0
lfu-log-factor 1}} {
    test "100K hits with lfu-log-factor 1 set lfu to 255" {
        set maxhit 100001
        for {set i 1} {$i < $maxhit} {incr i} {
            r incr foo
            if {100 == $i || 1000 == $i || 10000 == $i || 100000 == $i || 1000000 == $i} {
                puts "$i hits:[r object freq foo]"
            }
        }
        assert_equal 255 [r object freq foo]
    }
}

start_server {tags {"lfu"}
overrides {maxmemory 0}} {
    test "lfu count 22 decay to 11 divide by two" {
        r config set lfu-decay-time 0
        set foo 0
        while {[r object freq foo] < 22} {
            r incr foo
        }
        set freq [r object freq foo]
        r config set maxmemory 100M
        assert_equal 11 [r object freq foo]
    }

    test "lfu count 20 decay to 10 divide by two" {
        r config set maxmemory 0
        while {[r object freq foo] < 20} {
            r incr foo
        }

        set freq [r object freq foo]
        r config set maxmemory 100M
        assert_equal 10 [r object freq foo]
    }

    test "lfu count 12 decay to 10 by set to ten" {
        r config set maxmemory 0
        while {[r object freq foo] < 12} {
            r incr foo
        }

        set freq [r object freq foo]
        r config set maxmemory 100M
        assert_equal 10 [r object freq foo]
    }

    test "decr when lfu count <= 10" {
        while {[r object freq foo] > 0} {
            set freq [r object freq foo]
            r dumpfromssdb foo
            after 10
            assert_equal [expr $freq-1] [r object freq foo]
        }
    }

    test "keep 0 when lfu count == 0" {
        r dumpfromssdb foo
        after 10
        assert_equal 0 [r object freq foo]
    }
}

start_server {tags {"lfu"}
overrides {maxmemory 0}} {
    test "#crash issue Reply from SSDB:ERR value*integer*out of range" {
        r del foo
        r set foo bar
        dumpto_ssdb_and_wait r foo
        assert_error "*out of range*" {r incr foo}
        assert_error "*out of range*" {r incr foo}
        r get foo
    } {bar}

    test "new key's lfu is 5" {
        r del foo
        r set foo bar
        assert_equal 5 [r object freq foo] "[r object freq foo] not equal 5"
        r del foo
    }

    test "object freq can count key's lfu in ssdb" {
        r del foo
        r set foo bar
        dumpto_ssdb_and_wait r foo
        assert_equal 5 [r object freq foo] ""
        r del foo
    }

    test "#issue SWAP-9 lfu count be initiallized to 5 when key dump to ssdb " {
        r del foo
        for {set i 0} {$i < 1000} {incr i} {
            r incr foo
        }
        set freq [r object freq foo]
        assert {$freq > 5}
        dumpto_ssdb_and_wait r foo
        assert_equal $freq [r object freq foo] "ssdb freq:[r object freq foo] wrong"
    }

    test "#issue SWAP-20 lfu count be initiallized to 5 when key restore to redis " {
        set freq [r object freq foo]
        assert {$freq > 5}
        assert_equal 1000 [r get foo]
        assert_equal {redis} [r locatekey foo]
        assert {[r object freq foo] >= $freq}
    }
}
