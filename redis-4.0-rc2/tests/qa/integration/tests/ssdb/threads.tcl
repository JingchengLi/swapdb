start_server {tags {"ssdb"} } {
    test "multi threads access same key" {
        set host [srv host]
        set port [srv port]
        set num 100000
        set clist [start_bg_complex_data_list $host $port $num 100 samekey]
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
        set clist [start_bg_complex_data_list $host $port $num 100]
        after 5000
        stop_bg_client_list $clist
        # check redis still work
        after 200
        r ping
    } {PONG}
}
