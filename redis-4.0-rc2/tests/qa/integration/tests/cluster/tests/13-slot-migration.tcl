# Failover stress test.
# In this test a different node is killed in a loop for N
# iterations. The test checks that certain properties
# are preseved across iterations.

source "../tests/includes/init-tests.tcl"

test "Create a 5 nodes cluster" {
    create_cluster 5 0
}

test "Cluster is up" {
    assert_cluster_state ok
}

set numkeys 50000
set numops 0
set cluster [redis_cluster 127.0.0.1:[get_instance_attrib redis 0 port]]
catch {unset content}
array set content {}
set tribpid {}

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

test "Migrate slot without keys" {
    set slot [get_slot_by_key foo]
    set sourceId [get_id_by_slot $slot]
    set destinId [expr ($sourceId+1)%5]
    set keys_slot [R $sourceId cluster getkeysinslot $slot 100]
    while {[llength $keys_slot] > 0} {
        foreach key $keys_slot {
            $cluster del $key
        }
        set keys_slot [R $sourceId cluster getkeysinslot $slot 100]
    }
    R $sourceId cluster setslot $slot migrating [get_node_by_id $destinId]
    R $destinId cluster setslot $slot importing [get_node_by_id $sourceId]
    R $destinId cluster setslot $slot node [get_node_by_id $destinId]
    set_slot_addr_map $slot [get_nodeaddr_by_id $destinId]
    R $sourceId cluster setslot $slot stable
    R $destinId cluster setslot $slot stable

    $cluster set foo bar
    assert_equal {bar} [R $destinId get foo ] "redis instance get key"
    R $destinId set foo barbar
    assert_equal {barbar} [$cluster get foo ] "cluster get key"
}

test "Migrate slot with keys" {
    set slot [get_slot_by_key foo]
    set sourceId [get_id_by_slot $slot]
    set destinId [expr ($sourceId+1)%5]
    R $sourceId cluster setslot $slot migrating [get_node_by_id $destinId]
    R $destinId cluster setslot $slot importing [get_node_by_id $sourceId]
    lassign [split [get_nodeaddr_by_id $destinId] :] host port
    set keys_slot [R $sourceId cluster getkeysinslot $slot 100]
    while {[llength $keys_slot] > 0} {
        foreach key $keys_slot {
            assert_equal {OK} [ R $sourceId  migrate $host $port $key 0 100] "migrate should be ok"
        }
        set keys_slot [R $sourceId cluster getkeysinslot $slot 100]
    }
    R $destinId cluster setslot $slot node [get_node_by_id $destinId]
    set_slot_addr_map $slot [get_nodeaddr_by_id $destinId]
    R $sourceId cluster setslot $slot stable
    R $destinId cluster setslot $slot stable

    $cluster set foo bar
    assert_equal {bar} [R $destinId get foo ] "redis instance get key"
    R $destinId set foo barbar
    assert_equal {barbar} [$cluster get foo ] "cluster get key"
}

test "Verify $numkeys keys for consistency with logical content" {
    # Check that the Redis Cluster content matches our logical content.
    foreach {key value} [array get content] {
        if {[$cluster lrange $key 0 -1] ne $value} {
            fail "Key $key expected to hold '$value' but actual content is [$cluster lrange $key 0 -1]"
        }
    }
}
