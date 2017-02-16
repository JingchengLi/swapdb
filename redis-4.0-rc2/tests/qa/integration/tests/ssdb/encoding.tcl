#verify redis be cold(dumptossdb) and be hot(read) function
start_server {tags {"redis-ssdb"}} {
    set ssdb [redis $::host 8888]
    set redis [redis $::host 6379]

#string type
    foreach valtype {string-encoded integer-encoded mix-encoded} {
        if {$valtype == "string-encoded"} {
            set bar bar
        } elseif {$valtype == "integer-encoded"} {
            set bar 123
        } elseif {$valtype == "mix-encoded"} {
            set bar bar123
        }

        test "string $valtype Be Cold store in ssdb" {
            $redis del foo
            $redis set foo $bar
            $redis dumptossdb foo
            #wait key dumped to ssdb
            wait_for_condition 100 1 {
                [ $ssdb exists foo ] eq 1 
            } else {
                fail "key foo be dumptossdb failed"
            }
            assert_equal [$redis locatekey foo] {ssdb}
        }

        test "string $valtype Be Hot store in redis" {
            assert_equal $bar [ $redis get foo ]
            assert_equal [$redis locatekey foo] {redis}
            assert_equal {} [ $ssdb get foo ]
        }
    }

#hash type
    foreach encoding { ziplist hashtable} {
        if {$encoding == "ziplist"} {
            set num 8
        } elseif {$encoding == "hashtable"} {
            set num 1024
        }

        test "hash ($encoding) creation" {
            $redis del myhash
            array set myhash {}
            unset myhash
            for {set i 0} {$i < $num} {incr i} {
                set key __avoid_collisions__[randstring 0 8 alpha]
                set val __avoid_collisions__[randstring 0 8 alpha]
                if {[info exists myhash($key)]} {
                    incr i -1
                    continue
                }
                $redis hset myhash $key $val
                set myhash($key) $val
            }
            assert {[$redis object encoding myhash] eq $encoding}
        }

        test "hash ($encoding) Be Cold store in ssdb" {
            $redis dumptossdb myhash

            #wait key dumped to ssdb
            wait_for_condition 100 5 {
                [ $ssdb exists myhash ] eq 1 
            } else {
                fail "key myhash be dumptossdb failed"
            }

            assert_equal [$redis locatekey myhash] {ssdb}
            set err {}
            foreach k [array names myhash *] {
                if {$myhash($k) ne [$ssdb hget myhash $k]} {
                    set err "$myhash($k) != [$ssdb hget myhash $k]"
                    break
                }
            }
            set _ $err
        } {}

        test "hash ($encoding) Be Hot store in redis" {
            set err {}
            foreach k [array names myhash *] {
                if {$myhash($k) ne [$redis hget myhash $k]} {
                    set err "$myhash($k) != [$redis hget myhash $k]"
                    break
                }
            }
            assert_equal [$redis locatekey myhash] {redis}
            assert_equal 0 [ $ssdb exists myhash ]
            $redis del myhash
            set _ $err
        } {}
    }

# set type
    foreach valtype {string-encoded mix-encoded integer-encoded overflownumbers} \
        encoding { hashtable hashtable intset hashtable } {
        if {$valtype == "string-encoded"} {
            set list [list a b c d e f g]
        } elseif {$valtype == "mix-encoded"} {
            set list [list 1 2 3 4 5 6 7 a b c d e f g]
        } elseif {$valtype == "integer-encoded"} {
            set list [list 1 2 3 4 5 6 7]
        } elseif {$valtype == "overflownumbers"} {
            set list {}
            for {set i 0} {$i <=  512} {incr i} { 
                lappend list $i
            }
        }

        test "set ($encoding) $valtype creation" {
            $redis del myset
            foreach i $list {
                $redis sadd myset $i
            }
            assert {[$redis object encoding myset] eq $encoding}
        }

        test "set ($encoding) $valtype Be Cold store in ssdb" {
            $redis dumptossdb myset

            #wait key dumped to ssdb
            wait_for_condition 100 5 {
                [ $ssdb exists myset ] eq 1 
            } else {
                fail "key myset be dumptossdb failed"
            }

            assert_equal [$redis locatekey myset] {ssdb}
            assert_equal [lsort $list] [lsort [$ssdb smembers myset]]
        }

        test "set ($encoding) $valtype Be Hot store in redis" {
            assert_equal [lsort $list] [lsort [$redis smembers myset]]
            assert_equal [$redis locatekey myset] {redis}
            assert_equal 0 [ $ssdb exists myset ]
        }

        test {Be Cold/Hot with an expire key} {
            $redis expire myset 3
            $redis dumptossdb myset

            #wait key dumped to ssdb
            wait_for_condition 100 5 {
                [ $ssdb exists myset ] eq 1 
            } else {
                fail "key myset be dumptossdb failed"
            }

            assert_equal [$redis locatekey myset] {ssdb}
            assert_equal [lsort $list] [lsort [$ssdb smembers myset]]
            set ttl [$redis ttl myset]
            assert {$ttl >= 1 && $ttl <= 3}

            assert_equal [lsort $list] [lsort [$redis smembers myset]]
            assert_equal [$redis locatekey myset] {redis}
            assert {$ttl >= 1 && $ttl <= 3}
            $redis del myset
        }
    }

#zset type
    foreach encoding {ziplist skiplist} {
        if {$encoding == "ziplist"} {
            $redis config set zset-max-ziplist-entries 128
            $redis config set zset-max-ziplist-value 64
        } elseif {$encoding == "skiplist"} {
            $redis config set zset-max-ziplist-entries 1
            $redis config set zset-max-ziplist-value 1
        }

        test "zset ($encoding) creation" {
            $redis del myzset
            set list {}
            foreach i {1 2 3 4} j {a b c d} {
                $redis zadd myzset $i $j
                lappend list $j $i
            }
            assert {[$redis object encoding myzset] eq $encoding}
        }

        test "zset ($encoding) Be Cold store in ssdb" {
            $redis dumptossdb myzset

            #wait key dumped to ssdb
            wait_for_condition 100 5 {
                [ $ssdb exists myzset ] eq 1 
            } else {
                fail "key myzset be dumptossdb failed"
            }

            assert_equal [$redis locatekey myzset] {ssdb}
            assert_equal $list [$ssdb zrange myzset 0 -1 withscores]
        }

        test "zset ($encoding) Be Hot store in redis" {
            assert_equal $list [$redis zrange myzset 0 -1 withscores]
            assert_equal [$redis locatekey myzset] {redis}
            assert_equal 0 [ $ssdb exists myzset ]
        }

        test {Be Cold/Hot with an expire overflows a 32 bit integer key} {
            $redis pexpire myzset 2569591501
            set pttl [$redis pttl foottl]

            $redis dumptossdb myzset

            #wait key dumped to ssdb
            wait_for_condition 100 5 {
                [ $ssdb exists myzset ] eq 1 
            } else {
                fail "key myzset be dumptossdb failed"
            }

            assert_equal [$redis locatekey myzset] {ssdb}
            assert_equal $list [$ssdb zrange myzset 0 -1 withscores]
            set pttl [$redis pttl myzset]
            assert {$pttl >= 2569591501-3000 && $pttl <= 2569591501}

            assert_equal $list [$redis zrange myzset 0 -1 withscores]
            assert_equal [$redis locatekey myzset] {redis}
            set pttl [$redis pttl myzset]
            assert {$pttl >= 2569591501-3000 && $pttl <= 2569591501}

            $redis del myzset
        }
    }

#list type
    foreach valtype {string-encoded integer-encoded mix-encoded} {
        if {$valtype == "string-encoded"} {
            set list [list a b c d e f g]
        } elseif {$valtype == "integer-encoded"} {
            set list [list 1 2 3 4 5 6 7]
        } elseif {$valtype == "mix-encoded"} {
            set list [list 1 2 3 4 5 6 7 a b c d e f g]
        }
        
        test "list (quicklist) $valtype creation" {
            $redis del mylist
            foreach i $list {
                $redis lpush mylist $i
            }
            assert {[$redis object encoding mylist] eq {quicklist}}
        }

        test "list (quicklist) $valtype Be Cold store in ssdb" {
            $redis dumptossdb mylist

            #wait key dumped to ssdb
            wait_for_condition 100 5 {
                [ $ssdb exists mylist ] eq 1 
            } else {
                fail "key mylist be dumptossdb failed"
            }

            assert_equal [$redis locatekey mylist] {ssdb}
            assert_equal [lsort $list] [lsort [$ssdb lrange mylist 0 -1]]
        }

        test "list (quicklist) $valtype Be Hot store in redis" {
            assert_equal [lsort $list] [lsort [$redis lrange mylist 0 -1]]

            assert_equal [$redis locatekey mylist] {redis}
            assert_equal 0 [ $ssdb exists mylist ]
        }

        test {Override an key already exists in ssdb} {
            $ssdb set mylist barbusy

            $redis dumptossdb mylist

            #wait key dumped to ssdb
            wait_for_condition 100 5 {
                [ $ssdb exists mylist ] eq 1 
            } else {
                fail "key mylist be dumptossdb failed"
            }

            assert_equal [$redis locatekey mylist] {ssdb}
            assert_equal [lsort $list] [lsort [$ssdb lrange mylist 0 -1]]
        }
    }
}
