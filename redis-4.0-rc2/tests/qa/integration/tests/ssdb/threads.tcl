#    TODO write is blocked during load/transfer
#start_server {tags {"mtreads"}} {
#    test "#issue block timeout when multi threads incr key" {
#        r set foo 0
#        # dumpto_ssdb_and_wait r foo
#        exec ../../../build/redis-benchmark -q -p [srv port] -c 100 -n 20000 incr foo
#        assert_equal [expr 20000] [r get foo] "val should be right after multi threads incr key."
#    }
#}

start_server {tags {"threads"}} {
    test "#issue crash when multi threads spop same key" {
        exec ../../../build/redis-benchmark -q -p [srv port] -n 100 -t sadd,spop > /dev/null
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
