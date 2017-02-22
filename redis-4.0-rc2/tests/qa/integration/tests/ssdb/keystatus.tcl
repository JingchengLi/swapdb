start_server {tags {"ssdb"}} {
    set ssdb [redis $::host $::ssdbport]

    test "ssdb status key is in ssdb" {
        r set foo bar
        r dumptossdb foo
        wait_for_condition 100 1 {
            [ r locatekey foo ] eq {ssdb}
        } else {
            fail "key foo be dumptossdb failed"
        }
        list [$ssdb get foo] 
    } {bar}

    test "redis status key is not in ssdb" {
        r get foo
        wait_for_condition 100 1 {
            [ r locatekey foo ] eq {redis}
        } else {
            fail "key foo be hot failed"
        }
        list [$ssdb get foo] [ r get foo ]
    } {{} bar}

    test "status of key in ssdb is ssdb" {
        r dumptossdb foo
        wait_for_condition 100 1 {
            [ $ssdb get foo ] eq {bar}
        } else {
            fail "key foo be dumptossdb failed"
        }
        list [r locatekey foo] 
    } {ssdb}

    test "status of key in redis is redis" {
        r get foo
        wait_for_condition 100 1 {
            [$ssdb get foo] eq {}
        } else {
            fail "key foo be hot failed"
        }
        list [r locatekey foo] 
    } {redis}

    test "loop dump/restore multi times" {
        for {set i 0} {$i < 500} {incr i} {
            r set foo bar
            r dumptossdb foo
        }

        for {set i 0} {$i < 500} {incr i} {
            r dumptossdb foo
            r get foo
        }

        after 100

        assert { [r locatekey foo] ne {none} }
        if {[r locatekey foo] eq {redis}} {
            assert_equal [ $ssdb exists foo ] 0
        } elseif {[r locatekey foo] eq {ssdb}} {
            assert_equal [ $ssdb exists foo ] 1
        }
    }
    r del foo
}
