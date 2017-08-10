# TODO 内存满时优先淘汰冷数据 
start_server {tags {"lfu"}
overrides {maxmemory 0}} {

    proc sum_keystatus {pre nums status {flag 1}} {
        set sum 0
        for {set i 0} {$i < $nums} {incr i} {
            if {$flag && [r locatekey "$pre:$i"] eq $status} {
                incr sum
            } elseif {$flag == 0 && [r locatekey "$pre:$i"] ne $status} {
                incr sum
            }
        }
        return $sum
    }
        test "evict cold keys in prior when satisfy evicted condition" {
            set num 1000
            set val [string repeat 'x' 1024]
            for {set i 0} {$i < $num} {incr i} {
                r set "hot:$i" $val
                r set "cold:$i" $val
                r storetossdb "cold:$i"
            }
            wait_memory_stable
            set used [s used_memory]
            puts "used:$used"
            r config set ssdb-transfer-lower-limit 100
            r config set maxmemory [expr $used*3/4 ]
            wait_memory_stable

            puts [sum_keystatus "cold" $num none]
            puts [sum_keystatus "hot" $num none]
            after 1000000

    }
}
