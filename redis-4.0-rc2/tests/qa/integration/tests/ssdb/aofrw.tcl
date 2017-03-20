start_server {tags {"ssdb"}
overrides {
    save ""
    maxmemory 0
}} {

    test "Basic AOF rewrite" {
        r set foo bar
        r set hello world
        r storetossdb foo

        wait_for_dumpto_ssdb r foo

        r bgrewriteaof
        waitForBgrewriteaof r
        # Get the data set digest
        set d1 [r debug digest]

        # Load the AOF
        r debug loadaof
        set d2 [r debug digest]

        # Make sure they are the same
        assert {$d1 eq $d2}

        list [sr exists foo] [r get foo] [r locatekey hello] [r get hello]
    } {1 bar redis world}

    foreach d {string int} {
        foreach e {quicklist} {
            test "AOF rewrite of list with $e encoding, $d data" {
                r flushall

                set len 1000
                for {set j 0} {$j < $len} {incr j} {
                   if {$d eq {string}} {
                       set data [randstring 0 16 alpha]
                   } else {
                       set data [randomInt 4000000000]
                   }
                   r lpush key $data
                }

                for {set j 0} {$j < $len} {incr j} {
                   if {$d eq {string}} {
                       set data [randstring 0 16 alpha]
                   } else {
                       set data [randomInt 4000000000]
                   }
                   r lpush ssdbkey $data
                }
                set d3 [r debug digest]
                dumpto_ssdb_and_wait r ssdbkey

                assert_equal [r object encoding key] $e
                set d1 [r debug digest]
                r bgrewriteaof
                waitForBgrewriteaof r
                r debug loadaof
                set d2 [r debug digest]
                if {$d1 ne $d2} {
                    error "some key in ssdb assertion:$d1 is not equal to $d2"
                }

                assert {[r llen ssdbkey] eq $len}
                wait_for_restoreto_redis r ssdbkey

                set d4 [r debug digest]
                assert {$d1 ne $d3}
                if {$d3 ne $d4} {
                    error "some key restore to redis assertion:$d3 is not equal to $d4"
                }
                r del ssdbkey key
            }
        }
    }

    foreach d {string int} {
        foreach e {intset hashtable} {
            test "AOF rewrite of set with $e encoding, $d data" {
                r flushall
                #TODO del flushdb after flushall issue fixed
                sr flushdb
                # r del ssdbkey key
                if {$e eq {intset}} {set len 10} else {set len 1000}
                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    if {0 eq [ r sadd key $data ]} {
                        incr j -1
                    }
                }

                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    if {0 eq [ r sadd ssdbkey $data ]} {
                        incr j -1
                    }
                }
                set d3 [r debug digest]
                r storetossdb ssdbkey

                #wait key dumped to ssdb
                wait_for_condition 100 1 {
                    [ r locatekey ssdbkey ] eq {ssdb}
                } else {
                    fail "key ssdbkey be storetossdb failed"
                }

                if {$d ne {string}} {
                    assert_equal [r object encoding key] $e
                }
                set d1 [r debug digest]
                r bgrewriteaof
                waitForBgrewriteaof r
                r debug loadaof
                set d2 [r debug digest]
                if {$d1 ne $d2} {
                    error "some key in ssdb assertion:$d1 is not equal to $d2"
                }

                assert {[r scard ssdbkey] eq $len}
                #wait key restore to redis
                wait_for_condition 100 1 {
                    [ r locatekey ssdbkey ] eq {redis}
                } else {
                    fail "key ssdbkey restored failed"
                }
                set d4 [r debug digest]
                assert {$d1 ne $d3}
                if {$d3 ne $d4} {
                    error "some key restore to redis assertion:$d3 is not equal to $d4"
                }
                r del ssdbkey key
            }
        }
    }

    foreach d {string int} {
        foreach e {ziplist hashtable} {
            test "AOF rewrite of hash with $e encoding, $d data" {
                r flushall
                if {$e eq {ziplist}} {set len 10} else {set len 1000}
                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    if {0 eq [ r hset key $data $data]} {
                        incr j -1
                    }
                }

                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    if {0 eq [ r hset ssdbkey $data $data]} {
                        incr j -1
                    }
                }
                set d3 [r debug digest]
                r storetossdb ssdbkey

                #wait key dumped to ssdb
                wait_for_condition 100 1 {
                    [ r locatekey ssdbkey ] eq {ssdb}
                } else {
                    fail "key ssdbkey be storetossdb failed"
                }

                assert_equal [r object encoding key] $e
                set d1 [r debug digest]
                r bgrewriteaof
                waitForBgrewriteaof r
                r debug loadaof
                set d2 [r debug digest]
                if {$d1 ne $d2} {
                    error "some key in ssdb assertion:$d1 is not equal to $d2"
                }
                assert {[r hlen ssdbkey] eq $len}
                #wait key restore to redis
                wait_for_condition 100 1 {
                    [ r locatekey ssdbkey ] eq {redis}
                } else {
                    fail "key ssdbkey restored failed"
                }
                set d4 [r debug digest]
                assert {$d1 ne $d3}
                if {$d3 ne $d4} {
                    error "some key restore to redis assertion:$d3 is not equal to $d4"
                }
                r del ssdbkey key
            }
        }
    }

    foreach d {string int} {
        foreach e {ziplist skiplist} {
            test "AOF rewrite of zset with $e encoding, $d data" {
                r flushall
                sr flushdb
                if {$e eq {ziplist}} {set len 10} else {set len 1000}
                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    if {0 eq [ r zadd key [expr rand()] $data]} {
                        incr j -1
                    }
                }

                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    #Score accuracy in ssdb is limited to 4bits after dot.
                    #rand()*100000000 to avoid error
                    if {0 eq [ r zadd ssdbkey [expr int( rand()*100000000 )] $data]} {
                        incr j -1
                    }
                }

                set d3 [r debug digest]
                dumpto_ssdb_and_wait r ssdbkey

                assert_equal [r object encoding key] $e
                set d1 [r debug digest]
                r bgrewriteaof
                waitForBgrewriteaof r
                r debug loadaof
                set d2 [r debug digest]
                assert_equal $d1 $d2 "some key in ssdb assertion:$d1 is not equal to $d2"
                assert {[r zcard ssdbkey] eq $len}
                wait_for_restoreto_redis r ssdbkey

                set d4 [r debug digest]
                assert {$d1 ne $d3}
                assert_equal $d3 $d4 "some key restore to redis assertion:$d3 is not equal to $d4"
                r del ssdbkey key
            }
        }
    }
}
