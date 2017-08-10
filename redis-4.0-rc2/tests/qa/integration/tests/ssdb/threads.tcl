if {![file exists ../../../src/redis-benchmark]} {
    catch {exec make -C ../../../src redis-benchmark} info
}
start_server {tags {"mtreads"}} {
    test "single client incr key" {
        r set foo 0
        exec ../../../src/redis-benchmark -q -p [srv port] -c 1 -n 20000 incr foo
        assert_equal [expr 20000] [r get foo] "val should be right after one client incr key continuously."
    }
}

start_server {tags {"threads"}} {
    test "#issue crash when multi threads spop same key" {
        exec ../../../src/redis-benchmark -q -p [srv port] -n 100 -t sadd,spop > /dev/null
        r ping
    } {PONG}
}

start_server {tags {"ssdb"} } {
    test "multi threads access same key" {
        set host [srv host]
        set port [srv port]
        set num 100000
        set clist1 [start_bg_complex_data_list $host $port $num 100 {samekey useexpire}]
        after 5000
        stop_bg_client_list $clist1
        kill_ssdb_server
        restart_ssdb_server
        set clist [start_bg_complex_data_list $host $port $num 100 {samekey useexpire}]
        after 5000
        stop_bg_client_list $clist
        # check redis still work
        after 200
        r ping
    } {PONG}

    test "multi threads complex ops" {
        set host [srv host]
        set port [srv port]
        set num 100000
        set clist1 [start_bg_complex_data_list $host $port $num 50 useexpire]
        after 5000
        stop_bg_client_list $clist1
        kill_ssdb_server
        restart_ssdb_server
        set clist [start_bg_complex_data_list $host $port $num 50 useexpire]
        after 5000
        stop_bg_client_list $clist
        # check redis still work
        after 200
        r ping
    } {PONG}
}
