source tests/support/redis.tcl
source tests/support/util.tcl

proc bg_complex_data {host port db ops {opt {}}} {
    set r [redis $host $port]
    # $r select $db
    # $r select 0
    createComplexDataset $r $ops $opt
}

bg_complex_data [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [lindex $argv 4]
