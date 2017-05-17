# real config, during master loading
# 1. verify dataset identical after 10w times ops
# 2. verify dataset identical after 10w times ops with[out] expire keys.
start_server {tags {"repl"}
config {real.conf}} {
    start_server {config {real.conf}} {
        set master [srv -1 client]
        test "First server should have role slave after SLAVEOF" {
            r slaveof [srv -1 host] [srv -1 port]
            after 1000
            s role
        } {slave}

        # test "Slave should correctly synchronized" {
            # wait_for_online $master 1
        # }

        test "MASTER and SLAVE dataset should be identical after complex ops" {
            $master config set maxmemory 100M
            set pre [clock seconds]
            puts "start create"
            set keyslist [createComplexDataset $master 100000]
            puts "end create cost :[expr [clock seconds] - $pre ]"

            r -1 config set maxmemory 0
            foreach key $keyslist {
                wait_for_condition 50 100 {
                    [ r -1 exists $key ] eq [ r exists $key ]
                } else {
                    fail "key:$key in master and slave not identical"
                }
            }
            assert_equal [debug_digest r -1] [debug_digest r]
            wait_for_condition 50 100 {
                [llength [sr -1 keys *] ] eq 0
            } else {
                fail "master's ssdb keys should be null"
            }
            wait_for_condition 50 100 {
                [llength [sr keys *] ] eq 0
            } else {
                fail "slave's ssdb keys should be null"
            }
        }
    }
}

start_server {tags {"repl"}
config {real.conf}} {
    start_server {config {real.conf}} {
        set master [srv -1 client]
        test "First server should have role slave after SLAVEOF" {
            r slaveof [srv -1 host] [srv -1 port]
            wait_for_condition 50 100 {
                [s master_link_status] eq {up}
            } else {
                fail "Replication not started."
            }
        }

        set numops 100000

        test "MASTER and SLAVE consistency with expire" {
            $master config set maxmemory 100M
            set keyslist [ createComplexDataset $master $numops useexpire ]
            after 4000 ;# Make sure everything expired before taking the digest

            set oldmaxmemory [lindex [ r config get maxmemory ] 1]
            r config set maxmemory 0 ;# load all keys to redis
            foreach key $keyslist {
                wait_for_condition 50 100 {
                    [ r -1 exists $key ] eq [ r exists $key ]
                } else {
                    fail "key:$key in master and slave not identical [ r -1 exists $key ] [ r exists $key ]"
                }
            }
            after 1000 ;# Wait another second. Now everything should be fine.
            if {[r debug digest] ne [r -1 debug digest]} {
                set csv1 [csvdump r]
                set csv2 [csvdump {r -1}]
                set fd [open /tmp/repldump1.txt w]
                puts -nonewline $fd $csv1
                close $fd
                set fd [open /tmp/repldump2.txt w]
                puts -nonewline $fd $csv2
                close $fd
                puts "Master - Slave inconsistency"
                puts "Run diff -u against /tmp/repldump*.txt for more info"
            }
            assert_equal [r debug digest] [r -1 debug digest]
            r config set maxmemory $oldmaxmemory
        }
    }
}
