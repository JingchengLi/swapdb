start_server {tags {"ssdb"}} {
    set ssdb [redis $::host 8888]
    set redis [redis $::host 6379]

     test {MOVE basic usage} {
         $redis select 9
         $redis set mykey foobar
         $redis dumptossdb mykey
         $redis move mykey 10
         set res {}
         lappend res [$redis exists mykey]
         lappend res [$redis locatekey mykey]
         lappend res [$redis get mykey]
         lappend res [$redis dbsize]
         $redis select 10
         lappend res [$redis locatekey mykey]
         lappend res [$redis get mykey]
         lappend res [$redis locatekey mykey]
         lappend res [$redis dbsize]
         $redis select 9
         format $res
     } [list 0 none {} 0 ssdb foobar redis 1]
 
     test {MOVE against key existing in the target DB} {
         $redis select 10
         $redis set mykey foobar
         $redis dumptossdb mykey
         $redis select 9
         $redis set mykey hello
         $redis move mykey 10
     } {0}

     test {MOVE can move key expire metadata as well} {
         $redis select 10
         $redis flushdb
         $redis select 9
         $redis setex mykey 100 foo
         $redis dumptossdb mykey
         $redis move mykey 10
         assert {[$redis ttl mykey] == -2}
         $redis select 10
         assert {[$redis ttl mykey] > 0 && [$redis ttl mykey] <= 100}
         list [$redis locatekey mykey] [$redis get mykey]
     } {ssdb foo}

     test {MOVE does not create an expire if it does not exist} {
         $redis select 10
         $redis flushdb
         $redis select 9
         $redis set mykey foo
         $redis dumptossdb mykey
         $redis move mykey 10
         assert {[$redis ttl mykey] == -2}
         $redis select 10
         assert {[$redis ttl mykey] == -1}
         assert {[$redis get mykey] eq "foo"}
     }

    test "SET/DUMP/GET keys in different DBs" {
        $redis select 9
        $redis set a hello
        $redis dumptossdb a

        $redis set b world
        $redis dumptossdb b
        
        $redis select 10
        $redis set a foo
        $redis dumptossdb a
        $redis set b bared
        $redis dumptossdb b

        set res {}
        $redis select 9
        lappend res [$redis get a]
        lappend res [$redis get b]
        $redis select 10
        lappend res [$redis get a]
        lappend res [$redis get b]
        format $res
    } {hello world foo bared}
}
