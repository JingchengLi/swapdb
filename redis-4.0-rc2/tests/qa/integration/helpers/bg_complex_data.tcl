source support/redis.tcl
source support/util.tcl

proc bg_complex_data {host port db ops} {
    set r [redis $host $port]
    $r select $db
    createComplexDataset $r $ops
}

bg_complex_data [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3]
