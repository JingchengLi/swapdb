source tests/support/redis.tcl

proc start_hit_ssdb_tps {host port seconds} {
    set start_time [clock seconds]
    set r [redis $host $port 0]
    $r set keynull nullkey
    catch { $r storetossdb keynull }
    set rule [lindex [$r config get ssdb-load-rule] 1]
    lassign $rule time tps
    while 1 {
        for {set i 0} {$i < [expr $tps/$time*1.1]} {incr i} {
            catch { $r setlfu keynull 0 }
            catch { $r get keynull }
            # catch { $r setex keynull 2 nullkey }
        }
        after 1
        if {[clock seconds]-$start_time > $seconds} {
            catch {$r del keynull}
            exit 0
        }
    }
}

start_hit_ssdb_tps [lindex $argv 0] [lindex $argv 1] [lindex $argv 2]
