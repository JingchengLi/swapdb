start_server {tags {"ssdb"}} {
    set ssdb [redis $::host $::ssdbport]

     test {MOVE basic usage} {
         r select 9
         r set mykey foobar
         r dumptossdb mykey
         r move mykey 10
         set res {}
         lappend res [r exists mykey]
         lappend res [r locatekey mykey]
         lappend res [r get mykey]
         lappend res [r dbsize]
         r select 10
         lappend res [r locatekey mykey]
         lappend res [r get mykey]
         lappend res [r locatekey mykey]
         lappend res [r dbsize]
         r select 9
         format $res
     } [list 0 none {} 0 ssdb foobar redis 1]
 
     test {MOVE against key existing in the target DB} {
         r select 10
         r set mykey foobar
         r dumptossdb mykey
         r select 9
         r set mykey hello
         r move mykey 10
     } {0}

     test {MOVE can move key expire metadata as well} {
         r select 10
         r flushdb
         r select 9
         r setex mykey 100 foo
         r dumptossdb mykey
         r move mykey 10
         assert {[r ttl mykey] == -2}
         r select 10
         assert {[r ttl mykey] > 0 && [r ttl mykey] <= 100}
         list [r locatekey mykey] [r get mykey]
     } {ssdb foo}

     test {MOVE does not create an expire if it does not exist} {
         r select 10
         r flushdb
         r select 9
         r set mykey foo
         r dumptossdb mykey
         r move mykey 10
         assert {[r ttl mykey] == -2}
         r select 10
         assert {[r ttl mykey] == -1}
         assert {[r get mykey] eq "foo"}
     }

    test "SET/DUMP/GET keys in different DBs" {
        r select 9
        r set a hello
        r dumptossdb a

        r set b world
        r dumptossdb b
        
        r select 10
        r set a foo
        r dumptossdb a
        r set b bared
        r dumptossdb b

        set res {}
        r select 9
        lappend res [r get a]
        lappend res [r get b]
        r select 10
        lappend res [r get a]
        lappend res [r get b]
        format $res
    } {hello world foo bared}
}
