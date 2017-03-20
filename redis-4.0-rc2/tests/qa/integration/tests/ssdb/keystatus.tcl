start_server {tags {"ssdb"}
overrides {maxmemory 0}} {

    test "ssdb status key is in ssdb" {
        r set foo bar
        r storetossdb foo
        wait_for_condition 100 1 {
            [ r locatekey foo ] eq {ssdb}
        } else {
            fail "key foo be storetossdb failed"
        }
        list [sr get foo] 
    } {bar}

    test "redis status key is not in ssdb" {
        r get foo
        wait_for_condition 100 1 {
            [ r locatekey foo ] eq {redis}
        } else {
            fail "key foo be hot failed"
        }
        list [sr get foo] [ r get foo ]
    } {{} bar}

    test "status of key in ssdb is ssdb" {
        r storetossdb foo
        wait_for_condition 100 1 {
            [ sr get foo ] eq {bar}
        } else {
            fail "key foo be storetossdb failed"
        }
        list [r locatekey foo] 
    } {ssdb}

    test "status of key in redis is redis" {
        r get foo
        wait_for_condition 100 1 {
            [sr get foo] eq {}
        } else {
            fail "key foo be hot failed"
        }
        list [r locatekey foo] 
    } {redis}

    test "loop dump/restore multi times" {
        for {set i 0} {$i < 500} {incr i} {
            r set foo bar
            r storetossdb foo
        }

        for {set i 0} {$i < 500} {incr i} {
            r storetossdb foo
            r get foo
        }

        after 100

        assert { [r locatekey foo] ne {none} }
        if {[r locatekey foo] eq {redis}} {
            assert_equal [ sr exists foo ] 0
        } elseif {[r locatekey foo] eq {ssdb}} {
            assert_equal [ sr exists foo ] 1
        }
    }
    r del foo
}
