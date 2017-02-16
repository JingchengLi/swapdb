start_server {tags {"ssdb"}
overrides {save ""}} {
    set ssdb [redis $::host 8888]
    set redis [redis $::host 6379]

    test "Basic AOF rewrite" {
        $redis set foo bar
        $redis set hello world
        $redis dumptossdb foo

        #wait key dumped to ssdb
        wait_for_condition 100 1 {
            [ $ssdb exists foo ] eq 1
        } else {
            fail "key foo be dumptossdb failed"
        }
        # Get the data set digest
        set d1 [$redis debug digest]

        # Load the AOF
        $redis debug loadaof
        set d2 [$redis debug digest]

        # Make sure they are the same
        assert {$d1 eq $d2}

        list [$redis locatekey foo] [$redis get foo] [$redis locatekey hello] [$redis get hello]
    } {ssdb bar redis world}

    foreach d {string int} {
        foreach e {quicklist} {
            test "AOF rewrite of list with $e encoding, $d data" {
                $redis flushall

                set len 1000
                for {set j 0} {$j < $len} {incr j} {
                   if {$d eq {string}} {
                       set data [randstring 0 16 alpha]
                   } else {
                       set data [randomInt 4000000000]
                   }
                   $redis lpush key $data
                }

                for {set j 0} {$j < $len} {incr j} {
                   if {$d eq {string}} {
                       set data [randstring 0 16 alpha]
                   } else {
                       set data [randomInt 4000000000]
                   }
                   $redis lpush ssdbkey $data
                }
                set d3 [$redis debug digest]
                $redis dumptossdb ssdbkey

                #wait key dumped to ssdb
                wait_for_condition 100 1 {
                    [ $redis locatekey ssdbkey ] eq {ssdb}
                } else {
                    fail "key ssdbkey be dumptossdb failed"
                }

                assert_equal [$redis object encoding key] $e
                set d1 [$redis debug digest]
                $redis bgrewriteaof
                waitForBgrewriteaof $redis
                $redis debug loadaof
                set d2 [$redis debug digest]
                if {$d1 ne $d2} {
                    error "some key in ssdb assertion:$d1 is not equal to $d2"
                }

                assert {[$redis llen ssdbkey] eq $len}
                #wait key restore to redis
                wait_for_condition 100 1 {
                    [ $redis locatekey ssdbkey ] eq {redis}
                } else {
                    fail "key ssdbkey restored failed"
                }
                set d4 [$redis debug digest]
                assert {$d1 ne $d3}
                if {$d3 ne $d4} {
                    error "some key restore to redis assertion:$d3 is not equal to $d4"
                }
                $redis del ssdbkey key
            }
        }
    }

    foreach d {string int} {
        foreach e {intset hashtable} {
            test "AOF rewrite of set with $e encoding, $d data" {
                $redis flushall
                # $redis del ssdbkey key
                if {$e eq {intset}} {set len 10} else {set len 1000}
                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    if {0 eq [ $redis sadd key $data ]} {
                        # incr j -1
                    }
                }
                # if {$d eq "int" && $e eq "intset"} {
                    # return
# }

                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    if {0 eq [ $redis sadd ssdbkey $data ]} {
                        # incr j -1
                    }
                }
                set d3 [$redis debug digest]
                $redis dumptossdb ssdbkey

                #wait key dumped to ssdb
                wait_for_condition 100 1 {
                    [ $redis locatekey ssdbkey ] eq {ssdb}
                } else {
                    fail "key ssdbkey be dumptossdb failed"
                }

                if {$d ne {string}} {
                    assert_equal [$redis object encoding key] $e
                }
                set d1 [$redis debug digest]
                $redis bgrewriteaof
                waitForBgrewriteaof $redis
                $redis debug loadaof
                set d2 [$redis debug digest]
                if {$d1 ne $d2} {
                    error "some key in ssdb assertion:$d1 is not equal to $d2"
                }

                assert {[$redis scard ssdbkey] eq $len}
                #wait key restore to redis
                wait_for_condition 100 1 {
                    [ $redis locatekey ssdbkey ] eq {redis}
                } else {
                    fail "key ssdbkey restored failed"
                }
                set d4 [$redis debug digest]
                assert {$d1 ne $d3}
                if {$d3 ne $d4} {
                    error "some key restore to redis assertion:$d3 is not equal to $d4"
                }
                $redis del ssdbkey key
            }
        }
    }
    return

    foreach d {string int} {
        foreach e {ziplist hashtable} {
            test "AOF rewrite of hash with $e encoding, $d data" {
                $redis flushall
                if {$e eq {ziplist}} {set len 10} else {set len 1000}
                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    $redis hset key $data $data
                }

                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    $redis hset ssdbkey $data $data
                }
                set d3 [$redis debug digest]
                $redis dumptossdb ssdbkey

                #wait key dumped to ssdb
                wait_for_condition 100 1 {
                    [ $redis locatekey ssdbkey ] eq {ssdb}
                } else {
                    fail "key ssdbkey be dumptossdb failed"
                }

                assert_equal [$redis object encoding key] $e
                set d1 [$redis debug digest]
                $redis bgrewriteaof
                waitForBgrewriteaof $redis
                $redis debug loadaof
                set d2 [$redis debug digest]
                if {$d1 ne $d2} {
                    error "some key in ssdb assertion:$d1 is not equal to $d2"
                }
                assert {[$redis hlen ssdbkey] eq $len}
                #wait key restore to redis
                wait_for_condition 100 1 {
                    [ $redis locatekey ssdbkey ] eq {redis}
                } else {
                    fail "key ssdbkey restored failed"
                }
                set d4 [$redis debug digest]
                assert {$d1 ne $d3}
                if {$d3 ne $d4} {
                    error "some key restore to redis assertion:$d3 is not equal to $d4"
                }
                $redis del ssdbkey key
            }
        }
    }

    foreach d {string int} {
        foreach e {ziplist skiplist} {
            test "AOF rewrite of zset with $e encoding, $d data" {
                $redis flushall
                if {$e eq {ziplist}} {set len 10} else {set len 1000}
                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    $redis zadd key [expr rand()] $data
                }

                for {set j 0} {$j < $len} {incr j} {
                    if {$d eq {string}} {
                        set data [randstring 0 16 alpha]
                    } else {
                        set data [randomInt 4000000000]
                    }
                    $redis zadd ssdbkey [expr rand()] $data
                }
                set d3 [$redis debug digest]
                $redis dumptossdb ssdbkey

                #wait key dumped to ssdb
                wait_for_condition 100 1 {
                    [ $redis locatekey ssdbkey ] eq {ssdb}
                } else {
                    fail "key ssdbkey be dumptossdb failed"
                }

                assert_equal [$redis object encoding key] $e
                set d1 [$redis debug digest]
                $redis bgrewriteaof
                waitForBgrewriteaof $redis
                $redis debug loadaof
                set d2 [$redis debug digest]
                if {$d1 ne $d2} {
                    error "some key in ssdb assertion:$d1 is not equal to $d2"
                }
                assert {[$redis zcard ssdbkey] eq $len}
                #wait key restore to redis
                wait_for_condition 100 1 {
                    [ $redis locatekey ssdbkey ] eq {redis}
                } else {
                    fail "key ssdbkey restored failed"
                }
                set d4 [$redis debug digest]
                assert {$d1 ne $d3}
                if {$d3 ne $d4} {
                    error "some key restore to redis assertion:$d3 is not equal to $d4"
                }
                $redis del ssdbkey key
            }
        }
    }
}
