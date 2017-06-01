# Failover stress test.
# In this test a different node is killed in a loop for N
# iterations. The test checks that certain properties
# are preseved across iterations.

source "../tests/includes/init-tests.tcl"

proc migrate_slot {src des slot} {
    R $src cluster setslot $slot migrating [get_node_by_id $des]
    R $des cluster setslot $slot importing [get_node_by_id $src]

    lassign [split [get_nodeaddr_by_id $des] :] host port
    set keys_slot [R $src cluster getkeysinslot $slot 100]
    while {[llength $keys_slot] > 0} {
        foreach key $keys_slot {
            assert_equal {OK} [ R $src migrate $host $port $key 0 100] "migrate should be ok"
        }
        set keys_slot [R $src cluster getkeysinslot $slot 100]
    }
    foreach_redis_id id {
        R $id cluster setslot $slot node [get_node_by_id $des]
    }
    set_slot_addr_map $slot [get_nodeaddr_by_id $des]
    # after 1500
}

proc get_slot_with_keys {} {
    set retry 10000
    while {$retry} {
        set slot [randomInt 16384]
        set src [get_id_by_slot $slot]
        if {[ llength [R $src cluster getkeysinslot $slot 100] ] > 0} {
            return $slot
        }
        incr retry -1
    }
    error "No slot own keys found!!"
}


test "Create a 5 nodes cluster" {
    create_cluster 5 0
}

test "Cluster is up" {
    assert_cluster_state ok
}

set numkeys 50000
set numops 50000
set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis 0 port]]
catch {unset content}
array set content {}
set tribpid {}

test "Migrate slot without keys" {
    for {set n 0} {$n < 1} {incr n} {
        set key "key:[randomInt $numkeys]"
        set slot [get_slot_by_key $key]
        set sourceId [get_id_by_slot $slot]
        set destinId [expr ($sourceId+1)%5]
        # puts "$slot:$key:$sourceId->$destinId"
        set keys_slot [R $sourceId cluster getkeysinslot $slot 100]
        while {[llength $keys_slot] > 0} {
            foreach key $keys_slot {
                $cluster del $key
            }
            set keys_slot [R $sourceId cluster getkeysinslot $slot 100]
        }
        migrate_slot $sourceId $destinId $slot

        $cluster set $key bar
        assert_equal {bar} [R $destinId get $key ] "$slot:$key:$sourceId->$destinId:redis instance get key"
        R $destinId set $key barbar
        assert_equal {barbar} [$cluster get $key ] "$slot:$key:$sourceId->$destinId:cluster get key"
        R $destinId del $key
    }
}

test "Cluster fill keys" {
    set ele 0
    for {set j 0} {$j < $numops} {incr j} {

        # Write random data to random list.
        set listid [randomInt $numkeys]
        set key "key:$listid"
        incr ele
        $cluster rpush $key $ele
        lappend content($key) $ele

        if {($j % 1000) == 0} {
            puts -nonewline W; flush stdout
        }
    }
}

test "Migrate slot with keys" {
    for {set n 0} {$n < 1} {incr n} {
        set slot [ get_slot_with_keys ]
        set sourceId [get_id_by_slot $slot]
        set destinId [expr ($sourceId+1)%5]
        set key [lindex [R $sourceId cluster getkeysinslot $slot 1] 0]
        migrate_slot $sourceId $destinId $slot

        # puts "$sourceId->$destinId:$slot:$key"
        assert_equal $content($key) [ R $destinId lrange $key 0 -1 ] "$sourceId->$destinId:$slot:$key:read it before del and restore"
        set dump [R $destinId dump $key]
        R $destinId del $key
        R $destinId restore $key 0 $dump
        assert_equal $content($key) [ R $destinId lrange $key 0 -1 ] "$sourceId->$destinId:$slot:$key:read it after restore"
    }
}

test "Verify $numkeys keys for consistency with logical content" {
    # Check that the Redis Cluster content matches our logical content.
    foreach {key value} [array get content] {
        if {[$cluster lrange $key 0 -1] ne $value} {
            fail "Key $key expected to hold '$value' but actual content is [$cluster lrange $key 0 -1]"
        }
    }
}
