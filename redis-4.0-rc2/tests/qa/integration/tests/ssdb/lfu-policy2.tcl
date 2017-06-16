start_server {tags {"lfu"}} {
    set val [string repeat 'x' 1024]
    proc access_key_tps_time {key tps time {wait true}} {
        set begin [clock milliseconds]
        set n 0
        while {[expr ([clock milliseconds] - $begin)/1000 < $time]} {
            if {[expr ([clock milliseconds] - $begin)/1000 >= $n]} {
                for {set i 0} {$i < $tps} {incr i} {
                    r incr $key
                }
                incr n
                # break out when reach tps if wait == true
                if {false == $wait && $n == $time} {
                    break
                }
            }
            after 1
        }
    }

    foreach policy { allkeys-lfu } {
        r config set maxmemory-policy $policy
        test "ssdb-load-rule config" {
            set rule "1 100 10 800 60 3600"
            assert_equal {OK} [r config set ssdb-load-rule $rule] "set ssdb-load-rule"
            assert_equal $rule [lindex [r config get ssdb-load-rule] 1] "get ssdb-load-rule"
        }

        # load key when reach hits defined in rule and the lfu of key > 5
        r set foo 0 ;# key load when accessing with lfu > 5
        r set foo_1 0 ;# key not load with lfu <= 5
        r set foo_2 0 ;# key load when not accessing with lfu > 5

        set rule "1 100 5 400"
        r config set ssdb-load-rule $rule
        set rule [lindex [r config get ssdb-load-rule] 1]
        foreach {time tps} $rule {
            test "load key when satisfy rule $time $tps" {
                r storetossdb foo
                r storetossdb foo_1
                r storetossdb foo_2
                access_key_tps_time foo_2 [expr $tps/$time*0.5] 1;# make its lfu > 5
                access_key_tps_time foo [expr $tps/$time*0.9] $time
                assert_equal {ssdb} [r locatekey foo] "foo should not be loaded"
                assert_equal {ssdb} [r locatekey foo_1] "foo_2 should not be loaded"

                access_key_tps_time foo [expr $tps/$time*1.1] $time false
                r get foo_2 ;# TODO
                assert {[r object freq foo] > 5}
                assert {[r object freq foo_2] > 5}
                assert_equal {redis} [r locatekey foo] "foo should be loaded"
                # assert_equal {redis} [r locatekey foo_2] "foo_2 should be loaded" ;# TODO
                assert {[r object freq foo_1] <= 5}
                assert_equal {ssdb} [r locatekey foo_1] "foo_1 with lfu <= 5 should be loaded"
            }
        }
    }
}
