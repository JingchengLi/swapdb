start_server {tags {"lfu"}} {
    set val [string repeat 'x' 1024]
    proc incr_lfu_count_keys {pre nums count} {
        for {set i 0} {$i < $nums} {incr i} {
            for {set j 1} {$j < $count} {incr j} {
                r get "$pre:$i"
            }
        }
    }

    proc set_lfu_keys {pre nums lfu} {
        for {set i 0} {$i < $nums} {incr i} {
            r setlfu "$pre:$i" $lfu
        }
    }

    proc init_keys {pre nums} {
        set val [string repeat 'x' 1024]
        set sum 0
        for {set i 0} {$i < $nums} {incr i} {
            if {[r object freq "$pre:$i"] > 5} {
                incr sum
            }
            r del "$pre:$i"
            if {$i % 2} {
                r setex "$pre:$i" 10000 $val
            } else {
                r set "$pre:$i" $val
            }
            assert_equal 5 [r object freq "$pre:$i"] "del and set key should init key's lfu!"
        }
        return $sum
    }

    proc create_lfu_keys {pre nums count} {
        set val [string repeat 'x' 1024]
        for {set i 0} {$i < $nums} {incr i} {
            if {$i % 2} {
                r setex "$pre:$i" 10000 $val
            } else {
                r set "$pre:$i" $val
            }

            for {set j 1} {$j < $count} {incr j} {
                r get "$pre:$i"
            }
        }
    }

    proc sum_keystatus {pre nums status {flag 1}} {
        set sum 0
        for {set i 0} {$i < $nums} {incr i} {
            if {$flag && [r locatekey "$pre:$i"] eq $status} {
                incr sum
            } elseif {$flag == 0 && [r locatekey "$pre:$i"] ne $status} {
                incr sum
            }
        }
        return $sum
    }

    proc check_keys_load_when_access {pre nums flag} {
        for {set i 0} {$i < $nums} {incr i} {
            if {[r locatekey "$pre:$i"] eq {ssdb}} {
                r setlfu "$pre:$i" 5
                if {$flag} {
                    wait_for_condition 100 5 {
                        [r exists $pre:$i] && [r locatekey "$pre:$i"] eq "redis"
                    } else {
                        fail "$pre:$i not load success"
                    }
                } else {
                    r exists $pre:$i
                    assert_equal [r locatekey "$pre:$i"] ssdb "$pre:$i should not load"
                }
            }
        }
    }

    proc check_single_key_load_when_access {key {lfu_init 5}} {
        if {[r locatekey $key] eq {ssdb}} {
            r get $key
            if {[r object freq $key] > $lfu_init } {
                for {set n 0} {$n < 50} {incr n} {
                    r exists $key
                    if {[r locatekey "$key"] eq "redis"} {
                        break
                    }
                    after 10
                }
                assert_equal [r locatekey $key] redis "$key with freq [r object freq $key] not load success"
            } else {
                assert_equal [r locatekey $key] ssdb "$key with freq [r object freq $key] should not load"
            }
        }
    }

    proc check_keystatus {pre nums status} {
        for {set i 0} {$i < $nums} {incr i} {
            assert_equal [r locatekey "$pre:$i"] $status "$pre:$i status should be $status"
        }
    }

    foreach policy { allkeys-lfu } {
        # make sure to start with a blank instance
        r flushall

        r config set maxmemory-policy $policy
        set ssdb_transfer_limit 20
        r config set ssdb-transfer-lower-limit $ssdb_transfer_limit
        test "no storetossdb when lfu >= 5" {
            create_lfu_keys key_a 1000 5
            create_lfu_keys key_b 1000 1
            check_keystatus key_a 1000 redis
            check_keystatus key_b 1000 redis
        }

        test "no storetossdb when maxmemory is 0" {
            r config set maxmemory 0
            check_keystatus key_a 1000 redis
            check_keystatus key_b 1000 redis
        }

        wait_memory_stable
        set used [s used_memory]

        # Now add keys until the limit is almost reached.
        set numkey_c 0

        set limit [expr {int( (100.0/$ssdb_transfer_limit)*($used+1*1024*1024))}]
        r config set lfu-decay-time 0 ;# decay lfu counter
        test "no storetossdb when maxmemory usage lower than ssdb-transfer-lower-limit" {
            r config set maxmemory $limit
            while 1 {
                if {$numkey_c % 2} {
                    r setex "key_c:$numkey_c" 10000 $val
                } else {
                    r set "key_c:$numkey_c" $val
                }
                # Odd keys are volatile
                # Even keys are non volatile
                if {[expr [s used_memory]+292000 ]> $limit*($ssdb_transfer_limit/100.0)} {
                    assert {$numkey_c > 10}
                    break
                }
                incr numkey_c
            }
            wait_memory_stable
            # We should still be under the limit.
            assert {[s used_memory] < $limit*($ssdb_transfer_limit/100.0)}
            # memory usage near to the limit.
            assert {[s used_memory] > $limit*($ssdb_transfer_limit/100.0)*0.8}
            # However all our keys should be here.
            check_keystatus key_a 1000 redis
            check_keystatus key_b 1000 redis
            check_keystatus key_c $numkey_c redis
        }

        test "different key prefix with different lfu" {
            set_lfu_keys key_a 1000 200
            set_lfu_keys key_b 1000 100
            set_lfu_keys key_c $numkey_c 10
        }

        test "storetossdb when maxmemory usage higer than ssdb-transfer-lower-limit" {
            r config set lfu-decay-time 1
            set ssdb_transfer_limit [expr [ lindex [r config get ssdb-transfer-lower-limit] 1]+10]
            r config set ssdb-transfer-lower-limit $ssdb_transfer_limit
            
            wait_memory_stable
            create_lfu_keys key_d 3000 1
            r config set lfu-decay-time 0

            wait_memory_stable
            # We should still be under the limit.
            # puts "current limit:[expr double( [s used_memory] )/$limit ]"
            assert {[s used_memory] < $limit*($ssdb_transfer_limit/100.0)*1}
            # memory usage near to the limit.
            assert {[s used_memory] > $limit*($ssdb_transfer_limit/100.0)*0.5}
            # more frequently key with less probability to storetossdb
            set key_a_ssdb [sum_keystatus key_a 1000 ssdb]
            set key_d_ssdb [sum_keystatus key_d 3000 ssdb]
            assert {[expr double($key_a_ssdb)/1000] <= [expr double($key_d_ssdb)/3000]}
        }

        test "some key's lfu count decay" {
            # TODO currentlly not support config get lfu-decay-time
            # assert_equal 1 [lindex [ r config get lfu-decay-time ] 1] "make sure lfu-decay-time == 1"
            for {set i 0} {$i < 3000} {incr i} {
                if {[r object freq "key_d:$i"] < 4} {
                    break
                }
                assert {$i != 2999} "some key_d's lfu count should changed"
            }
        }

        test "no keys evicted out" {
            assert_equal [ sum_keystatus key_a 1000 none ] 0
            assert_equal [ sum_keystatus key_b 1000 none ] 0
            assert_equal [ sum_keystatus key_c $numkey_c none ] 0
            assert_equal [ sum_keystatus key_d 3000 none ] 0
        }

        test "no load from ssdb when no reach the load tps access ssdb" {
            set ssdb_load_upper_limit 100
            r config set ssdb-load-upper-limit $ssdb_load_upper_limit
           
            set ssdbnums [expr [ sum_keystatus key_a 1000 ssdb ]+\
            [ sum_keystatus key_b 1000 ssdb ]+\
            [ sum_keystatus key_c $numkey_c ssdb ]+\
            [ sum_keystatus key_d 3000 ssdb ]]
            assert { $ssdbnums > 0 }

            check_keys_load_when_access key_a 1000 0
            check_keys_load_when_access key_b 1000 0
            check_keys_load_when_access key_c $numkey_c 0
            check_keys_load_when_access key_d 3000 0
        }

        # reach load tps access ssdb
        set handle [ start_hit_ssdb_tps [srv host] [srv port] 200 ]
        test "no load from ssdb when maxmemory usage higer than ssdb-load-upper-limit" {
            set ssdb_load_upper_limit 10
            assert {[s used_memory] > $limit*($ssdb_load_upper_limit/100.0)*1}
            r config set ssdb-load-upper-limit $ssdb_load_upper_limit
           
            set ssdbnums [expr [ sum_keystatus key_a 1000 ssdb ]+\
            [ sum_keystatus key_b 1000 ssdb ]+\
            [ sum_keystatus key_c $numkey_c ssdb ]+\
            [ sum_keystatus key_d 3000 ssdb ]]
            assert { $ssdbnums > 0 }

            check_keys_load_when_access key_a 1000 0
            check_keys_load_when_access key_b 1000 0
            check_keys_load_when_access key_c $numkey_c 0
            check_keys_load_when_access key_d 3000 0
        }

        test "load keys from ssdb when maxmemory is 0" {
            r config set maxmemory 0
            r config set lfu-decay-time 1
            # access_key_tps_time foo 200 1 false ;# reach the load tps access ssdb

            after 1000
            check_keys_load_when_access key_a 1000 1
            check_keys_load_when_access key_d 3000 1

            check_keystatus key_a 1000 redis
            check_keystatus key_d 3000 redis
        }

        test "load keys from ssdb when maxmemory usage lower than ssdb-load-upper-limit" {
            #TODO
            set ssdb_transfer_limit 100 ;# disable dumptossdb
            wait_memory_stable
            set used [s used_memory]
            set limit [expr 3*$used]
            set ssdb_load_upper_limit 80
            r config set ssdb-load-upper-limit $ssdb_load_upper_limit

            check_keys_load_when_access key_b 1000 1
            check_keys_load_when_access key_c $numkey_c 1

            check_keystatus key_b 1000 redis
            check_keystatus key_c $numkey_c redis
            wait_memory_stable
            assert {[s used_memory] < $limit*($ssdb_load_upper_limit/100.0)*1}
            set ssdb_transfer_limit 30 ;# enable dumptossdb
        }

        test "del and set key should init key's lfu!" {
            #some key's lfu count should > 5
            assert { [ init_keys "key_d" 3000 ] > 0 } 
        }

        test "lfu-decay-time = 0 not decr key's lfu count when not satisfy storetossdb condition" {
            r config set lfu-decay-time 0
            after 500
            for {set i 0} {$i < 3000} {incr i} {
                assert_equal 5 [r object freq "key_d:$i"] "key_d:$i's lfu count changed"
            }
        }

        test "lfu-decay-time = 0 decr key's lfu count when satisfy storetossdb condition" {
            #Now lower-limit is 30, upper-limit is 10
            set used [s used_memory]
            set limit [expr 3*$used]
            r config set maxmemory $limit

            wait_memory_stable
            set flag 0
            for {set num 0} {$num < 3000} {incr num} {
                if {5 > [r object freq "key_d:$num"]} {
                    r storetossdb "key_d:$num"
                    set flag 1
                    # break
                }
            }

            assert {$flag eq 1}
            r config set lfu-decay-time 1
        }

        test "freq < 5 can not be load into redis" {
            # all the key_b:xxx that storetossdb should be with freq < 5
            set ssdb_transfer_limit 100 ;# disable dumptossdb
            r config set ssdb-transfer-lower-limit $ssdb_transfer_limit
            set ssdb_load_upper_limit 60
            r config set ssdb-load-upper-limit $ssdb_load_upper_limit

            check_keys_load_when_access key_d 3000 0
        }

        test "only freq > 5 can be load into redis" {
            set key_d_ssdb_before [sum_keystatus key_d 3000 ssdb]
            for {set i 0} {$i < 3000} {incr i} {
                check_single_key_load_when_access "key_d:$i"
            }
            set key_d_ssdb_after [sum_keystatus key_d 3000 ssdb]
            assert {$key_d_ssdb_before > $key_d_ssdb_after}
        }
        stop_write_load $handle

        test "keys with more frequently access less possible storetossdb" {
            #Now lower-limit is 100, upper-limit is 60
            r flushall
            create_lfu_keys key_a 3000 1
            create_lfu_keys key_b 3000 1
            create_lfu_keys key_c 3000 1
            create_lfu_keys key_d 3000 1
            set_lfu_keys key_a 3000 4
            set_lfu_keys key_b 3000 3
            set_lfu_keys key_c 3000 2
            set_lfu_keys key_d 3000 1
            wait_memory_stable
            set used [s used_memory]
            set limit [expr 2*$used]
            set ssdb_transfer_limit 30
            r config set ssdb-transfer-lower-limit $ssdb_transfer_limit
            r config set maxmemory $limit
            wait_memory_stable
            assert {[s used_memory] < $limit*($ssdb_transfer_limit/100.0)*1}
            # memory usage near to the limit.
            assert {[s used_memory] > $limit*($ssdb_transfer_limit/100.0)*0.7}
            set key_a_ssdb [sum_keystatus key_a 3000 ssdb]
            set key_b_ssdb [sum_keystatus key_b 3000 ssdb]
            set key_c_ssdb [sum_keystatus key_c 3000 ssdb]
            set key_d_ssdb [sum_keystatus key_d 3000 ssdb]
            assert {[expr double($key_a_ssdb)/3000] <= [expr double($key_b_ssdb)/3000]}
            assert {[expr double($key_b_ssdb)/3000] <= [expr double($key_c_ssdb)/3000]}
            assert {[expr double($key_c_ssdb)/3000] <= [expr double($key_d_ssdb)/3000]}
        }
    }
}
