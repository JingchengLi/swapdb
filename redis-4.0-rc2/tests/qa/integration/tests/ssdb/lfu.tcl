start_server {tags {"lfu"}} {
    test "lfu-log-factor config" {
        set factor 1
        assert_equal {OK} [r config set lfu-log-factor $factor] "set lfu-log-factor"
        assert_equal $factor [lindex [r config get lfu-log-factor] 1] "get lfu-log-factor"
    }

    test "lfu-decay-time config" {
        set decaytime 2
        assert_equal {OK} [r config set lfu-decay-time $decaytime] "set lfu-decay-time"
        assert_equal $decaytime [lindex [r config get lfu-decay-time] 1] "get lfu-decay-time"
    }

    test "ssdb-transfer-lower-limit config" {
        set lower 20
        assert_equal {OK} [r config set ssdb-transfer-lower-limit $lower] "set ssdb-transfer-lower-limit"
        assert_equal $lower [lindex [r config get ssdb-transfer-lower-limit] 1] "get ssdb-transfer-lower-limit"
    }

    test "ssdb-load-upper-limit config" {
        set upper 50
        assert_equal {OK} [r config set ssdb-load-upper-limit $upper] "set ssdb-load-upper-limit"
        assert_equal $upper [lindex [r config get ssdb-load-upper-limit] 1] "get ssdb-load-upper-limit"
    }
}

start_server {tags {"lfu"}
overrides {maxmemory 0}} {
    test "100K hits with lfu-log-factor 10 default" {
        set maxhit 100001
        for {set i 1} {$i < $maxhit} {incr i} {
            r incr foo
            if {100 == $i || 1000 == $i || 10000 == $i || 100000 == $i || 1000000 == $i} {
                puts "$i hits:[r object freq foo]"
            }
        }
        assert { 255 > [r object freq foo] }
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

# need wait one minute to run this test, only enable it when accurate set.
if {$::accurate} {
    start_server {tags {"lfu"}
    overrides { maxmemory 0
        lfu-decay-time 1}} {
        test "initialize keys lfu count" {
            while {[r object freq foo22] < 22} {
                r incr foo22
            }

            while {[r object freq foo20] < 20} {
                r incr foo20
            }

            while {[r object freq foo12] < 12} {
                r incr foo12
            }

            for {set var 0} {$var <= 10} {incr var} {
                r set foo$var 0
                r setlfu foo$var $var
            }

            r config set maxmemory 100M
            after 60500
        }

        test "lfu count 22 decay to 11 divide by two" {
            assert_equal 11 [r object freq foo22]
        }

        test "lfu count 20 decay to 10 divide by two" {
            assert_equal 10 [r object freq foo20]
        }

        test "lfu count 12 decay to 10 by set to ten" {
            assert_equal 10 [r object freq foo12]
        }

        test "decr when lfu 6 <= count <= 10 and keep in redis" {
            for {set var 6} {$var <= 10} {incr var} {
                assert_equal [expr $var-1] [r object freq foo$var]
                assert_equal {redis} [r locatekey foo$var]
            }
        }

        test "decr when lfu 5 == count and store to ssdb" {
            assert_equal 4 [r object freq foo5]
            assert_equal {ssdb} [r locatekey foo5]
        }

        test "keep same when lfu count <= 4 and store to ssdb" {
            for {set var 0} {$var <= 4} {incr var} {
                assert_equal [expr $var] [r object freq foo$var]
                assert_equal {ssdb} [r locatekey foo$var]
            }
        }
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
        r dumpfromssdb foo ;# TODO
        assert_equal {redis} [r locatekey foo]
        assert {[r object freq foo] >= $freq}
    }
}
