source "./ssdb/init_ssdb.tcl"
start_server {tags {"ssdb-other"}} {
    test {APPEND basics} {
        r del foo
        list [r append foo bar] [r get foo] \
             [r append foo 100] [r get foo]
    } {3 bar 6 bar100}

    test {APPEND basics, integer encoded values} {
        set res {}
        r del foo
        r append foo 1
        r append foo 2
        lappend res [r get foo]
        r set foo 1
        r append foo 2
        lappend res [r get foo]
    } {12 12}

    test {APPEND fuzzing} {
        set err {}
        foreach type {binary alpha compr} {
            set buf {}
            r del x
            for {set i 0} {$i < 1000} {incr i} {
                set bin [randstring 0 10 $type]
                append buf $bin
                r append x $bin
            }
            if {$buf != [r get x]} {
                set err "Expected '$buf' found '[r get x]'"
                break
            }
        }
        set _ $err
    } {}

    test {type basics} {
        assert_type none fooxxxx

        r set foo bar
        assert_type string foo
        r del foo
        assert_type none foo

        r hset myhash field hello
        assert_type hash myhash
        r del myhash
        assert_type none myhash

        r zadd myzset 1 one
        assert_type zset myzset
        r del myzset
        assert_type none myzset

        r sadd myset 1
        assert_type set myset
        r del myset
        assert_type none myset

        r lpush mylist a
        assert_type list mylist
        r del mylist
        assert_type none mylist
    }
}
