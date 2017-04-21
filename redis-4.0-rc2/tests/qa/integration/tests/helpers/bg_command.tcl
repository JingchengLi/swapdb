source tests/support/redis.tcl

proc bg_command {host port args} {
    set r [redis $host $port]
    # $r select 0
    catch {[$r {*}[lrange {*}$args 0 end]]} err
    # ERR there is already another flushll task processing
    if {![string match "*ERR*another*task*" $err]} {
        puts "Log:$err"
    }
}

bg_command [lindex $argv 0] [lindex $argv 1] [lindex $argv 2]
