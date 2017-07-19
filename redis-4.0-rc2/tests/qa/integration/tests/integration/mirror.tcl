# ssdb在写入运行过程中，发生重启, 保证数据不丢失

#disable this case that dirty key should be processed by clients when master ssdb restart.
return
start_server {tags {"repl-abnormal"}} {
    start_server {} {

        set nodeA [srv -1 client]
        set nodeA_host [srv -1 host]
        set nodeA_port [srv -1 port]
        set m_client [srv 0 client]
        set m_host [srv 0 host]
        set m_port [srv 0 port]
        set num 100000
        start_bg_mirror_complex $nodeA_host $nodeA_port $m_host $m_port $num
        after 2000

        test "nodeA ssdb restart during writing" {
            for {set n 0} {$n < 3} {incr n} {
                after 500
                kill_ssdb_server -1
                after 1000
                restart_ssdb_server -1
            }
            set nodeA [srv -1 client]
            after 500
            $nodeA set stopflag true
            $nodeA config set maxmemory 0
            $m_client config set maxmemory 0
            compare_debug_digest
        }
    }
}
