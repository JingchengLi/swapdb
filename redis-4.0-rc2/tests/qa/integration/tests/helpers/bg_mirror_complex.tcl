source tests/support/redis.tcl
source tests/support/util.tcl

proc createMirrorComplexDataset {r mr ops {opt {}}} {
    set keyslist {}
    for {set j 0} {$j < $ops} {incr j} {
        if {$j%100 == 0} {
            if {[$r get stopflag] eq "true"} {
                break
            }
        }
        if {[lsearch -exact $opt samekey] != -1} {
            set k "key_8888888888"
        } elseif {[lsearch -exact $opt incr] != -1} {
            $r incr incr_key
            $mr incr incr_key
            continue
         } else {
            randpath {
                set k [randomKey]
            } {
                set k [lindex $keyslist [randomInt [llength $keyslist]]]
            }
        }
        lappend keyslist $k
        set f [randomValue]
        if {[lsearch -exact $opt 10M] != -1} {
            set v [string repeat x 10000000]
        } elseif {[lsearch -exact $opt 1M] != -1} {
            set v [string repeat x 1000000]
        } elseif {[lsearch -exact $opt 100k] != -1} {
            set v [string repeat x 100000]
        } elseif {[lsearch -exact $opt 10k] != -1} {
            set v [string repeat x 100000]
        } elseif {[lsearch -exact $opt 1k] != -1} {
            set v [string repeat x 1000]
        } else {
            set v [randomValue]
        }

        set d [randomSignedInt 100000000]
        catch {
            set t [{*}$r type $k]
            if {$t eq {none}} {
                randpath {
                    {*}$r set $k $v
                    {*}$mr set $k $v
                } {
                    {*}$r lpush $k $v
                    {*}$mr lpush $k $v
                } {
                    {*}$r sadd $k $v
                    {*}$mr sadd $k $v
                } {
                    {*}$r zadd $k $d $v
                    {*}$mr zadd $k $d $v
                } {
                    {*}$r hset $k $f $v
                    {*}$mr hset $k $f $v
                } {
                    {*}$r del $k
                    {*}$mr del $k
                }
                set t [{*}$r type $k]
            }
        } err;# WRONGTYPE ERR

        catch {
            switch $t {
                {string} {
                    randpath {
                        {*}$r set $k $v
                        {*}$mr set $k $v
                    } {
                        {*}$r set $k ${v}_diff
                        {*}$mr set $k ${v}_diff
                    }
                }
                {list} {
                    randpath {
                        {*}$r lpush $k $v
                        {*}$mr lpush $k $v
                    } {
                        {*}$r rpush $k $v
                        {*}$mr rpush $k $v
                    } {
                        {*}$r rpush $k ${v}_diff
                        {*}$mr rpush $k ${v}_diff
                    } {
                        {*}$r rpop $k
                        {*}$mr rpop $k
                    } {
                        {*}$r lpop $k
                        {*}$mr lpop $k
                    }
                }
                {set} {
                    randpath {
                        {*}$r sadd $k $v
                        {*}$mr sadd $k $v
                    } {
                        {*}$r sadd $k ${v}_diff
                        {*}$mr sadd $k ${v}_diff
                    } {
                        {*}$r srem $k $v
                        {*}$mr srem $k $v
                    }
                }
                {zset} {
                    randpath {
                        {*}$r zadd $k $d $v
                        {*}$mr zadd $k $d $v
                    } {
                        {*}$r zadd $k $d ${v}_diff
                        {*}$mr zadd $k $d ${v}_diff
                    } {
                        {*}$r zrem $k $v
                        {*}$mr zrem $k $v
                    }
                }
                {hash} {
                    randpath {
                        {*}$r hset $k $f $v
                        {*}$mr hset $k $f $v
                    } {
                        {*}$r hset $k $f ${v}_diff
                        {*}$mr hset $k $f ${v}_diff
                    } {
                        {*}$r hdel $k $f
                        {*}$mr hdel $k $f
                    }
                }
            }
        } err;# WRONGTYPE ERR

        if {rand() < 0.5} {
            set delaytime 1000
            randpath {
               set dtime [expr 1000*$delaytime+[randomInt 10]] 
               {*}$r pexpire $k $dtime
               {*}$mr pexpire $k $dtime
            } {
               set dtime [expr $delaytime+[randomInt 5] ]
                {*}$r expire $k $dtime
                {*}$mr expire $k $dtime
            } {
               set dtime [expr $delaytime+[clock seconds] +[randomInt 10000]]
                {*}$r expireat $k $dtime
                {*}$mr expireat $k $dtime
            } {
                set dtime [expr 1000*$delaytime+[clock milliseconds] +[randomInt 1000]]
                {*}$r pexpireat $k $dtime
                {*}$mr pexpireat $k $dtime
            } {
                set dtime [expr $delaytime+1000+[randomInt 10000]]
                {*}$r expire $k $dtime
                {*}$mr expire $k $dtime
            }
            if {{string} eq $t} {
                catch {
                    randpath {
                        set dtime [expr $delaytime+1000+[randomInt 10000]]
                        {*}$r setex $k $dtime ${v}_volatile
                        {*}$mr setex $k $dtime ${v}_volatile
                    } {
                        set dtime [expr $delaytime+1000+[randomInt 10000]]
                        {*}$r set $k ${v}_volatile EX $dtime
                        {*}$mr set $k ${v}_volatile EX $dtime
                    } {
                        set dtime [expr 1000*$delaytime+[randomInt 1000]]
                        {*}$r set $k ${v}_volatile PX $dtime
                        {*}$mr set $k ${v}_volatile PX $dtime
                    }
               } err;
            }
        }
    }
    return [lsort -unique $keyslist ]
}

proc bg_mirror_complex {host port shost sport ops {opt {}}} {
    set r [redis $host $port]
    set mr [redis $shost $sport]
    createMirrorComplexDataset $r $mr $ops $opt
}

bg_mirror_complex [lindex $argv 0] [lindex $argv 1] [lindex $argv 2] [lindex $argv 3] [lindex $argv 4] [lindex $argv 5]
