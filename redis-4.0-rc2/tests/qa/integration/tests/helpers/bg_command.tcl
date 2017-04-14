source tests/support/redis.tcl

proc bg_command {host port args} {
    set r [redis $host $port]
    # $r select 0
    $r {*}[lrange {*}$args 0 end]
}

bg_command [lindex $argv 0] [lindex $argv 1] [lindex $argv 2]
