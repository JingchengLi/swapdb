start_server {
    tags {"type"}
    overrides {
        maxmemory 0
        "list-max-ziplist-size" 4
    }
} {
    source "tests/unit/type/list-common.tcl"

    foreach {type large} [array get largevalue] {
        tags {"slow"} {
            test "LTRIM stress testing - $type" {
                set mylist {}
                set startlen 32
                ssdbr del mylist

                # Start with the large value to ensure the
                # right encoding is used.
                ssdbr rpush mylist $large
                lappend mylist $large

                for {set i 0} {$i < $startlen} {incr i} {
                    set str [randomInt 9223372036854775807]
                    ssdbr rpush mylist $str
                    lappend mylist $str
                }

                if {$::accurate} {
                    set loops 500
                } else {
                    set loops 1
                }
                for {set i 0} {$i < $loops} {incr i} {
                    set min [expr {int(rand()*$startlen)}]
                    set max [expr {$min+int(rand()*$startlen)}]
                    set before_len [llength $mylist]
                    set before_len_r [ssdbr llen mylist]
                    set mylist [lrange $mylist $min $max]
                    ssdbr ltrim mylist $min $max
                    assert_equal $mylist [ssdbr lrange mylist 0 -1] "failed trim"

                    set starting [ssdbr llen mylist]
                    for {set j [ssdbr llen mylist]} {$j < $startlen} {incr j} {
                        set str [randomInt 9223372036854775807]
                        ssdbr rpush mylist $str
                        lappend mylist $str
                        assert_equal $mylist [ssdbr lrange mylist 0 -1] "failed append match"
                    }
                }
            }
        }
    }
}
