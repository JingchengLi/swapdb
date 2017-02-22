start_server {tags {"ssdb"}} {
    set ssdb [redis $::host $::ssdbport]

    test "load-from-ssdb default is yes" {
        lindex [ r config get load-from-ssdb ] 1
    } {yes}

    foreach {maxmemory} {0 1000M} {
        r config set maxmemory $maxmemory
        test "dump keys to ssdb - maxmemory($maxmemory)" {
            r set foo1 bar1
            r set foo2 bar2
            r set foo3 bar3
            r dumptossdb foo1
            r dumptossdb foo2
            r dumptossdb foo3

            wait_for_condition 100 5 {
                [ $ssdb get foo1 ] eq {bar1}
            } else {
                fail "key foo1 be dumptossdb failed"
            }

            wait_for_condition 100 5 {
                [ $ssdb get foo2 ] eq {bar2}
            } else {
                fail "key foo2 be dumptossdb failed"
            }

            wait_for_condition 100 5 {
                [ $ssdb get foo3 ] eq {bar3}
            } else {
                fail "key foo3 be dumptossdb failed"
            }
            list [r locatekey foo1] [r locatekey foo2] [r locatekey foo3]
        } {ssdb ssdb ssdb}

        test "Get make key hot if load-from-ssdb(yes) - maxmemory($maxmemory)" {
            r config set load-from-ssdb yes
            assert {[lindex [r config get load-from-ssdb] 1] eq yes} 

            r get foo1

            wait_for_condition 100 5 {
                [ $ssdb get foo1 ] eq {}
            } else {
                fail "key foo1 be hot failed"
            }

            list [r locatekey foo1] [r locatekey foo2] [r locatekey foo3]
        } {redis ssdb ssdb}

        test "Set make key hot if load-from-ssdb(yes) - maxmemory($maxmemory)" {
            r config set load-from-ssdb yes
            assert {[lindex [r config get load-from-ssdb] 1] eq yes} 

            r get foo3

            wait_for_condition 100 5 {
                [ $ssdb get foo3 ] eq {}
            } else {
                fail "key foo1 be hot failed"
            }

            list [r locatekey foo1] [r locatekey foo2] [r locatekey foo3]
        } {redis ssdb redis}

        test "Get not make key hot if load-from-ssdb(no) - maxmemory($maxmemory)" {
            r config set load-from-ssdb no
            assert {[lindex [r config get load-from-ssdb] 1] eq no} 

            r get foo2
            list [r locatekey foo2] [$ssdb get foo2]
        } {ssdb bar2}

        test "Set not make key hot if load-from-ssdb(no) - maxmemory($maxmemory)" {
            r config set load-from-ssdb no
            assert {[lindex [r config get load-from-ssdb] 1] eq no} 

            r set foo2 bar2
            list [r locatekey foo2] [$ssdb get foo2]
        } {ssdb bar2}
        # currently not support multi-keys command
        # r del foo1 foo2
        test "Can del keys if load-from-ssdb(no) - maxmemory($maxmemory)" {
            r del foo1
            r del foo2
            r del foo3
            list [r get foo1] [r get foo2] [r get foo3] [r locatekey foo1] [r locatekey foo2] [r locatekey foo3]
        } {{} {} {} none none none}
        r config set load-from-ssdb yes
    }
}
