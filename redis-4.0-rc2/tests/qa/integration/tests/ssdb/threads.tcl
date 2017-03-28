start_server {tags {"ssdb"} } {
    test "multi threads access same key" {
        set host [srv host]
        set port [srv port]
        set num 10
        set load_handle0 [start_bg_complex_data $host $port 0 $num samekey]
        set load_handle1 [start_bg_complex_data $host $port 0 $num samekey]
        set load_handle2 [start_bg_complex_data $host $port 0 $num samekey]
        after 500
        stop_bg_complex_data $load_handle0
        stop_bg_complex_data $load_handle1
        stop_bg_complex_data $load_handle2
        # check redis still work
        r ping
    } {PONG}
}
