start_server {tags {"ssdb"}} {
    set ssdb [redis $::host 8888]
    set redis [redis $::host 6379]

    test "load-from-ssdb default is yes" {
        lindex [ $redis config get load-from-ssdb ] 1
    } {yes}

    foreach {maxmemory} {0 1000M} {
        $redis config set maxmemory $maxmemory
        test "dump keys to ssdb - maxmemory($maxmemory)" {
            $redis set foo1 bar1
            $redis set foo2 bar2
            $redis set foo3 bar3
            $redis dumptossdb foo1
            $redis dumptossdb foo2
            $redis dumptossdb foo3

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
            list [$redis locatekey foo1] [$redis locatekey foo2] [$redis locatekey foo3]
        } {ssdb ssdb ssdb}

        test "Get make key hot if load-from-ssdb(yes) - maxmemory($maxmemory)" {
            $redis config set load-from-ssdb yes
            assert {[lindex [$redis config get load-from-ssdb] 1] eq yes} 

            $redis get foo1

            wait_for_condition 100 5 {
                [ $ssdb get foo1 ] eq {}
            } else {
                fail "key foo1 be hot failed"
            }

            list [$redis locatekey foo1] [$redis locatekey foo2] [$redis locatekey foo3]
        } {redis ssdb ssdb}

        test "Set make key hot if load-from-ssdb(yes) - maxmemory($maxmemory)" {
            $redis config set load-from-ssdb yes
            assert {[lindex [$redis config get load-from-ssdb] 1] eq yes} 

            $redis get foo3

            wait_for_condition 100 5 {
                [ $ssdb get foo3 ] eq {}
            } else {
                fail "key foo1 be hot failed"
            }

            list [$redis locatekey foo1] [$redis locatekey foo2] [$redis locatekey foo3]
        } {redis ssdb redis}

        test "Get not make key hot if load-from-ssdb(no) - maxmemory($maxmemory)" {
            $redis config set load-from-ssdb no
            assert {[lindex [$redis config get load-from-ssdb] 1] eq no} 

            $redis get foo2
            list [$redis locatekey foo2] [$ssdb get foo2]
        } {ssdb bar2}

        test "Set not make key hot if load-from-ssdb(no) - maxmemory($maxmemory)" {
            $redis config set load-from-ssdb no
            assert {[lindex [$redis config get load-from-ssdb] 1] eq no} 

            $redis set foo2 bar2
            list [$redis locatekey foo2] [$ssdb get foo2]
        } {ssdb bar2}
        # currently not support multi-keys command
        # $redis del foo1 foo2
        test "Can del keys if load-from-ssdb(no) - maxmemory($maxmemory)" {
            $redis del foo1
            $redis del foo2
            $redis del foo3
            list [$redis get foo1] [$redis get foo2] [$redis get foo3] [$redis locatekey foo1] [$redis locatekey foo2] [$redis locatekey foo3]
        } {{} {} {} none none none}
        $redis config set load-from-ssdb yes
    }
}
