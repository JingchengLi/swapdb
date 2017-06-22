# ssdb-load-rule
start_server {tags {"lfu"}
overrides {maxmemory 0}} {
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

        set rule ""
        r config set ssdb-load-rule $rule
        test "no load key with no rule" {
            r storetossdb foo
            access_key_tps_time foo 500 1
            assert_equal {ssdb} [r locatekey foo] "foo should not be loaded"
        }

        set rule "1 100"
        r config set ssdb-load-rule $rule
        set rule [lindex [r config get ssdb-load-rule] 1]
        lassign $rule time tps
        test "load key when satisfy rule $time $tps" {
            r storetossdb foo
            r storetossdb foo_1
            dumpto_ssdb_and_wait r foo_2
            r setlfu foo_2 10;# make its lfu > 5
            r get foo_2
            access_key_tps_time foo [expr $tps/$time*0.9] $time
            assert_equal {ssdb} [r locatekey foo] "foo should not be loaded"
            assert_equal {ssdb} [r locatekey foo_2] "foo_2 should not be loaded"

            access_key_tps_time foo [expr $tps/$time*1.1] $time false
            assert {[r object freq foo] > 5}
            assert {[r object freq foo_2] > 5}
            assert_equal {redis} [r locatekey foo] "foo should be loaded"
            assert_equal {redis} [r locatekey foo_2] "foo_2 should be loaded"
            assert {[r object freq foo_1] <= 5}
            assert_equal {ssdb} [r locatekey foo_1] "foo_1 with lfu <= 5 should not be loaded"
        }

        test "no load key when satisfy rule $time $tps but no access key" {
            r storetossdb foo
            r storetossdb foo_2
            access_key_tps_time foo [expr $tps/$time*1.1] $time
            assert {[r object freq foo_2] > 5}
            assert_equal {redis} [r locatekey foo] "foo should be loaded"
            assert_equal {ssdb} [r locatekey foo_2] "foo_2 should not be loaded without access"
        }

        test "no load key when satisfy rule $time $tps but lfu <= 5" {
            r storetossdb foo
            r storetossdb foo_1
            r storetossdb foo_2
            r setlfu foo_1 4
            r get foo_1
            r get foo_2
            assert {[r object freq foo_1] <= 5}
            assert {[r object freq foo_2] > 5}
            access_key_tps_time foo [expr $tps/$time*1.1] $time false
            assert_equal {ssdb} [r locatekey foo_1] "foo_1 with lfu <= 5 should not be loaded"
            assert_equal {redis} [r locatekey foo_2] "foo_2 with lfu > 5 should be loaded"
        }

        set rule "1 150"
        r config set ssdb-load-rule $rule
        set rule [lindex [r config get ssdb-load-rule] 1]
        lassign $rule time tps
        test "new load rule overrides the old rule" {
            r storetossdb foo
            access_key_tps_time foo [expr $tps/$time*0.9] $time
            assert_equal {ssdb} [r locatekey foo] "foo should not be loaded"
            access_key_tps_time foo [expr $tps/$time*1.1] $time
            assert_equal {redis} [r locatekey foo] "foo should be loaded"
        }

        set rule "1 100 10 800"
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
                assert_equal {ssdb} [r locatekey foo_2] "foo_2 should not be loaded"

                access_key_tps_time foo [expr $tps/$time*1.1] $time false
                assert {[r object freq foo] > 5}
                assert {[r object freq foo_2] > 5}
                assert_equal {redis} [r locatekey foo] "foo should be loaded"
                assert_equal {redis} [r locatekey foo_2] "foo_2 should be loaded" ;# TODO
                assert {[r object freq foo_1] <= 5}
                assert_equal {ssdb} [r locatekey foo_1] "foo_1 with lfu <= 5 should be loaded"
            }
        }
    }
}
