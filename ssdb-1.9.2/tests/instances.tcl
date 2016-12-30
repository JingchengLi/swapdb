# Multi-instance test framework.
# This is used in order to test Sentinel and Redis Cluster, and provides
# basic capabilities for spawning and handling N parallel Redis / Sentinel
# instances.
#
# Copyright (C) 2014 Salvatore Sanfilippo antirez@gmail.com
# This software is released under the BSD License. See the COPYING file for
# more information.

package require Tcl 8.5

set tcl_precision 17
source ../support/redis.tcl
source ../support/util.tcl
source ../support/server.tcl
source ../support/test.tcl

set ::verbose 0
set ::valgrind 0
set ::pause_on_error 0
set ::simulate_error 0
set ::failed 0
set ::redis_instances {}
set ::pids {} ; # We kill everything at exit
set ::dirs {} ; # We remove all the temp dirs at exit
set ::run_matching {} ; # If non empty, only tests matching pattern are run.

if {[catch {cd tmp}]} {
    puts "tmp directory not found."
    puts "Please run this test from the Redis source root."
    exit 1
}

proc log_crashes {} {
    set start_pattern {*REDIS BUG REPORT START*}
    set logs [glob */log.txt]
    foreach log $logs {
        set fd [open $log]
        set found 0
        while {[gets $fd line] >= 0} {
            if {[string match $start_pattern $line]} {
                puts "\n*** Crash report found in $log ***"
                set found 1
            }
            if {$found} {puts $line}
        }
    }
}

proc cleanup {} {
    puts "Cleaning up..."
    log_crashes
    foreach pid $::pids {
        catch {exec kill -9 $pid}
    }
    foreach dir $::dirs {
        catch {exec rm -rf $dir}
    }
}


proc parse_options {} {
    for {set j 0} {$j < [llength $::argv]} {incr j} {
        set opt [lindex $::argv $j]
        set val [lindex $::argv [expr $j+1]]
        if {$opt eq "--single"} {
            incr j
            set ::run_matching "*${val}*"
        } elseif {$opt eq "--pause-on-error"} {
            set ::pause_on_error 1
        } elseif {$opt eq "--fail"} {
            set ::simulate_error 1
        } elseif {$opt eq {--valgrind}} {
            set ::valgrind 1
        } elseif {$opt eq "--help"} {
            puts "Hello, I'm sentinel.tcl and I run Sentinel unit tests."
            puts "\nOptions:"
            puts "--single <pattern>      Only runs tests specified by pattern."
            puts "--pause-on-error        Pause for manual inspection on error."
            puts "--fail                  Simulate a test failure."
            puts "--valgrind              Run with valgrind."
            puts "--help                  Shows this help."
            exit 0
        } else {
            puts "Unknown option $opt"
            exit 1
        }
    }
}

# If --pause-on-error option was passed at startup this function is called
# on error in order to give the developer a chance to understand more about
# the error condition while the instances are still running.
proc pause_on_error {} {
    puts ""
    puts [colorstr yellow "*** Please inspect the error now ***"]
    puts "\nType \"continue\" to resume the test, \"help\" for help screen.\n"
    while 1 {
        puts -nonewline "> "
        flush stdout
        set line [gets stdin]
        set argv [split $line " "]
        set cmd [lindex $argv 0]
        if {$cmd eq {continue}} {
            break
        } elseif {$cmd eq {show-redis-logs}} {
            set count 10
            if {[lindex $argv 1] ne {}} {set count [lindex $argv 1]}
            foreach_redis_id id {
                puts "=== REDIS $id ===="
                puts [exec tail -$count redis_$id/log.txt]
                puts "---------------------\n"
            }
        } elseif {$cmd eq {show-sentinel-logs}} {
            set count 10
            if {[lindex $argv 1] ne {}} {set count [lindex $argv 1]}
            foreach_sentinel_id id {
                puts "=== SENTINEL $id ===="
                puts [exec tail -$count sentinel_$id/log.txt]
                puts "---------------------\n"
            }
        } elseif {$cmd eq {ls}} {
            foreach_redis_id id {
                puts -nonewline "Redis $id"
                set errcode [catch {
                    set str {}
                    append str "@[RI $id tcp_port]: "
                    append str "[RI $id role] "
                    if {[RI $id role] eq {slave}} {
                        append str "[RI $id master_host]:[RI $id master_port]"
                    }
                    set str
                } retval]
                if {$errcode} {
                    puts " -- $retval"
                } else {
                    puts $retval
                }
            }
            foreach_sentinel_id id {
                puts -nonewline "Sentinel $id"
                set errcode [catch {
                    set str {}
                    append str "@[SI $id tcp_port]: "
                    append str "[join [S $id sentinel get-master-addr-by-name mymaster]]"
                    set str
                } retval]
                if {$errcode} {
                    puts " -- $retval"
                } else {
                    puts $retval
                }
            }
        } elseif {$cmd eq {help}} {
            puts "ls                     List Sentinel and Redis instances."
            puts "show-sentinel-logs \[N\] Show latest N lines of logs."
            puts "show-redis-logs \[N\]    Show latest N lines of logs."
            puts "S <id> cmd ... arg     Call command in Sentinel <id>."
            puts "R <id> cmd ... arg     Call command in Redis <id>."
            puts "SI <id> <field>        Show Sentinel <id> INFO <field>."
            puts "RI <id> <field>        Show Sentinel <id> INFO <field>."
            puts "continue               Resume test."
        } else {
            set errcode [catch {eval $line} retval]
            if {$retval ne {}} {puts "$retval"}
        }
    }
}

# We redefine 'test' as for Sentinel we don't use the server-client
# architecture for the test, everything is sequential.
proc test {descr code} {
    set ts [clock format [clock seconds] -format %H:%M:%S]
    puts -nonewline "$ts> $descr: "
    flush stdout

    if {[catch {set retval [uplevel 1 $code]} error]} {
        incr ::failed
        if {[string match "assertion:*" $error]} {
            set msg [string range $error 10 end]
            puts [colorstr red $msg]
            if {$::pause_on_error} pause_on_error
            puts "(Jumping to next unit after error)"
            return -code continue
        } else {
            # Re-raise, let handler up the stack take care of this.
            error $error $::errorInfo
        }
    } else {
        puts [colorstr green OK]
    }
}


# Execute all the units inside the 'tests' directory.
proc run_tests {} {
    set tests [lsort [glob ../tests/*]]
    foreach test $tests {
        if {$::run_matching ne {} && [string match $::run_matching $test] == 0} {
            continue
        }
        if {[file isdirectory $test]} continue
        puts [colorstr yellow "Testing unit: [lindex [file split $test] end]"]
        source $test
    }
}

# Print a message and exists with 0 / 1 according to zero or more failures.
proc end_tests {} {
    if {$::failed == 0} {
        puts "GOOD! No errors."
        exit 0
    } else {
        puts "WARNING $::failed tests faield."
        exit 1
    }
}
