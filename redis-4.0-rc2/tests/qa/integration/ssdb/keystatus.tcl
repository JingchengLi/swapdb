# exec killall -9 redis-server
# should start a ssdb-server first.
start_server {tags {"ssdb"}} {
    #TODO ssdb should start when redis start
    set ssdbcfgfile ./ssdb/redis_with_ssdb.conf
    set ssdbpid [exec ../../../src/redis-server $ssdbcfgfile > /dev/null &]

    after 1000
    set ssdb [redis 127.0.0.1 8888]
    set redis [redis 127.0.0.1 6379]

    test "ssdb status key is in ssdb" {
        $redis set foo bar
        $redis dumptossdb foo
        wait_for_condition 100 1 {
            [ $redis locatekey foo ] eq {ssdb}
        } else {
            fail "key foo be dumptossdb failed"
        }
        list [$ssdb get foo] 
    } {bar}

    test "redis status key is not in ssdb" {
        $redis get foo
        wait_for_condition 100 1 {
            [ $redis locatekey foo ] eq {redis}
        } else {
            fail "key foo be hot failed"
        }
        list [$ssdb get foo] [ $redis get foo ]
    } {{} bar}

    test "status of key in ssdb is ssdb" {
        $redis dumptossdb foo
        wait_for_condition 100 1 {
            [ $ssdb get foo ] eq {bar}
        } else {
            fail "key foo be dumptossdb failed"
        }
        list [$redis locatekey foo] 
    } {ssdb}

    test "status of key in redis is redis" {
        $redis get foo
        wait_for_condition 100 1 {
            [$ssdb get foo] eq {}
        } else {
            fail "key foo be hot failed"
        }
        list [$redis locatekey foo] 
    } {redis}

    test "loop dump/restore multi times" {
        for {set i 0} {$i < 500} {incr i} {
            $redis set foo bar
            $redis dumptossdb foo
        }

        for {set i 0} {$i < 500} {incr i} {
            $redis dumptossdb foo
            $redis get foo
        }

        wait_for_condition 200 1 {
            [ $redis locatekey foo ] eq {redis}
        } else {
            fail "key foo be hot failed"
        }
        list [$ssdb get foo] [ $redis get foo ]
    } {{} bar}
    catch {exec kill -9 $ssdbpid}
}
