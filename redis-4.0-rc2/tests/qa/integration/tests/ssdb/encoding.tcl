#verify redis be cold(dumptossdb) and be hot(read) function
start_server {tags {"redis-ssdb"}} {
    set ssdb [redis $::host $::ssdbport]

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
            r del foo
            r set foo $bar
            r dumptossdb foo
            wait_for_dumpto_ssdb r foo

            assert_equal [$ssdb get foo] $bar
        }

        test "string $valtype Be Hot store in redis" {
            assert_equal $bar [ r get foo ]
            wait_for_restoreto_redis r foo
            assert_equal {} [ $ssdb get foo ]
        }
        r del foo
    }

#hash type
    foreach encoding { ziplist hashtable} {
        if {$encoding == "ziplist"} {
            set num 8
        } elseif {$encoding == "hashtable"} {
            set num 1024
        }

        test "hash ($encoding) creation" {
            r del myhash
            array set myhash {}
            unset myhash
            for {set i 0} {$i < $num} {incr i} {
                set key __avoid_collisions__[randstring 0 8 alpha]
                set val __avoid_collisions__[randstring 0 8 alpha]
                if {[info exists myhash($key)]} {
                    incr i -1
                    continue
                }
                r hset myhash $key $val
                set myhash($key) $val
            }
            assert {[r object encoding myhash] eq $encoding}
        }

        test "hash ($encoding) Be Cold store in ssdb" {
            r dumptossdb myhash

            wait_for_dumpto_ssdb r myhash

            assert_equal [$ssdb exists myhash] {1}
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
                if {$myhash($k) ne [r hget myhash $k]} {
                    set err "$myhash($k) != [r hget myhash $k]"
                    break
                }
            }
            assert_equal [r locatekey myhash] {redis}
            assert_equal 0 [ $ssdb exists myhash ]
            r del myhash
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
            r del myset
            foreach i $list {
                r sadd myset $i
            }
            assert {[r object encoding myset] eq $encoding}
        }

        test "set ($encoding) $valtype Be Cold store in ssdb" {
            r dumptossdb myset

            wait_for_dumpto_ssdb r myset

            assert_equal [$ssdb exists myset] {1}
            assert_equal [lsort $list] [lsort [$ssdb smembers myset]]
        }

        test "set ($encoding) $valtype Be Hot store in redis" {
            assert_equal [lsort $list] [lsort [r smembers myset]]
            assert_equal [r locatekey myset] {redis}
            assert_equal [ $ssdb exists myset ] 0
        }

        test {Be Cold/Hot with an expire key} {
            r expire myset 3
            r dumptossdb myset

            wait_for_dumpto_ssdb r myset

            assert_equal [$ssdb exists myset] {1}
            assert_equal [lsort $list] [lsort [$ssdb smembers myset]]
            set ttl [r ttl myset]
            assert {$ttl >= 1 && $ttl <= 3}

            assert_equal [lsort $list] [lsort [r smembers myset]]
            assert_equal [r locatekey myset] {redis}
            assert {$ttl >= 1 && $ttl <= 3}
            r del myset
        }
    }

#zset type
    foreach encoding {ziplist skiplist} {
        if {$encoding == "ziplist"} {
            r config set zset-max-ziplist-entries 128
            r config set zset-max-ziplist-value 64
        } elseif {$encoding == "skiplist"} {
            r config set zset-max-ziplist-entries 1
            r config set zset-max-ziplist-value 1
        }

        test "zset ($encoding) creation" {
            r del myzset
            set list {}
            foreach i {1 2 3 4} j {a b c d} {
                r zadd myzset $i $j
                lappend list $j $i
            }
            assert {[r object encoding myzset] eq $encoding}
        }

        test "zset ($encoding) Be Cold store in ssdb" {
            r dumptossdb myzset

            wait_for_dumpto_ssdb r myzset

            assert_equal [$ssdb exists myzset] {1}
            assert_equal $list [$ssdb zrange myzset 0 -1 withscores]
        }

        test "zset ($encoding) Be Hot store in redis" {
            assert_equal $list [r zrange myzset 0 -1 withscores]
            assert_equal [r locatekey myzset] {redis}
            assert_equal 0 [ $ssdb exists myzset ]
        }

        test {Be Cold/Hot with an expire overflows a 32 bit integer key ($encoding)} {
            r pexpire myzset 2569591501
            set pttl [r pttl foottl]

            r dumptossdb myzset

            wait_for_dumpto_ssdb r myzset

            assert_equal [$ssdb exists myzset] {1}
            assert_equal $list [$ssdb zrange myzset 0 -1 withscores]
            set pttl [r pttl myzset]
            assert {$pttl >= 2569591501-3000 && $pttl <= 2569591501}

            assert_equal $list [r zrange myzset 0 -1 withscores]
            assert_equal [r locatekey myzset] {redis}
            set pttl [r pttl myzset]
            assert {$pttl >= 2569591501-3000 && $pttl <= 2569591501}

            r del myzset
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
            r del mylist
            foreach i $list {
                r lpush mylist $i
            }
            assert {[r object encoding mylist] eq {quicklist}}
        }

        test "list (quicklist) $valtype Be Cold store in ssdb" {
            r dumptossdb mylist

            wait_for_dumpto_ssdb r mylist

            assert_equal [$ssdb exists mylist] {1}
            assert_equal [lsort $list] [lsort [$ssdb lrange mylist 0 -1]]
        }

        test "list (quicklist) $valtype Be Hot store in redis" {
            assert_equal [lsort $list] [lsort [r lrange mylist 0 -1]]

            assert_equal [r locatekey mylist] {redis}
            assert_equal [ $ssdb exists mylist ] 0
        }

        test {Override an key already exists in ssdb} {
            $ssdb set mylist barbusy

            r dumptossdb mylist

            wait_for_dumpto_ssdb r mylist

            assert_equal [$ssdb exists mylist] {1}
            assert_equal [lsort $list] [lsort [$ssdb lrange mylist 0 -1]]
        }
        r del mylist
    }
}
