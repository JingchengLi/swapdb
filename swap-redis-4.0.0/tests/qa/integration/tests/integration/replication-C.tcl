proc write_ops {r} {
    set num 0
    for {set var 0} {$var < 3} {incr var} {
        catch { $r lpush foo test } ret
        if {![string match {*ERR*} $ret]} {
            incr num
        }
        after 1
    }
    return $num
}

start_server {tags {"repl"}} {
    set master [srv 0 client]
    start_server {} {
        set slave [srv 0 client]
        foreach keystimeout {50000 1} {
            $slave config set client-blocked-by-keys-timeout $keystimeout
            test "MASTER and SLAVE should be identical when block timeout during Master and Slave($keystimeout)" {
                $master config set client-blocked-by-keys-timeout 1
                $slave slaveof [srv -1 host] [srv -1 port]
                wait_for_online $master 1
                $master config set maxmemory 0
                $master del foo
                for {set j 0} {$j < 20000} {incr j} {
                    $master rpush foo 1 2 3 4 5 6 7 8 9 10
                    $master rpush foo "item 1" "item 2" "item 3" "item 4" "item 5" \
                    "item 6" "item 7" "item 8" "item 9" "item 10"
                }
                $master config set maxmemory 100M
                set num1 [write_ops $master]
                # assert_equal {redis} [$master locatekey foo] "transfer should fail when key timeout"
                $master config set client-blocked-by-keys-timeout 50000
                set num2 [write_ops $master]
                # assert_equal {ssdb} [$master locatekey foo] "transfer should success when no key timeout"
                $master config set maxmemory 0
                $master config set client-blocked-by-keys-timeout 1
                $master dumpfromssdb foo
                set num3 [write_ops $master]
                # assert_equal {ssdb} [$master locatekey foo] "load should fail when key timeout"
                $master config set client-blocked-by-keys-timeout 50000
                $master dumpfromssdb foo
                set num4 [write_ops $master]
                # assert_equal {redis} [$master locatekey foo] "load should success when no key timeout"
                assert_equal [expr 400000+$num1+$num2+$num3+$num4] [$master llen foo] "All operation return no ERR should success."
                wait_for_condition 10 200 {
                    [$master llen foo] eq [$slave llen foo]
                } else {
                    fail "Master([$master llen foo]) and Slave([$slave llen foo]) not identical"
                }
            }
        }
    }
}
