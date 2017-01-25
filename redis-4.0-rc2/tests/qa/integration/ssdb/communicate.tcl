# exec killall -9 redis-server
#TODO:Need API to verify key only in ssdb, not in redis. maybe new 
#parameter dbsize can record redis/ssdb keys number.
start_server {tags {"ssdb"}} {
    #TODO ssdb should start when redis start
    set ssdbcfgfile ./ssdb/redis_with_ssdb.conf
    set nossdbcfgfile ./ssdb/redis_without_ssdb.conf
    set ssdbpid [exec ../../../src/redis-server $ssdbcfgfile &]
    set nossdbpid 0
    set redis_no_ssdb {}

    after 1000
    set ssdb [redis 127.0.0.1 8888]
    set redis [redis 127.0.0.1 6379]
    #start a test redis-server not connect to ssdb.

    proc get_total_calls { s ssdb } {
        set info [$ssdb info]
        set len [string length $s]
        set start [string first $s $info]
        set end [string first " " $info [expr $start+$len+1 ]]
        string range $info [expr $start+$len+1 ] $end-1
    } 

    test "Ssdb is up" {
        $ssdb ping
    } {PONG}

    test "ToDo Ssdb connect to redis" {
        #TODO
        #redis should have some flag/status to check this
    }

    test "Hot key only store in redis" {
        $redis del foo
        
        $redis set foo bar
        list [$redis locatekey foo] [ $ssdb get foo ] [ $redis get foo ] 
    } {redis {} bar}

    test "Key(become cold) move from redis to ssdb" {
        $redis dumptossdb foo
        $redis bgsave
        set nossdbpid [exec ../../../src/redis-server $nossdbcfgfile &]
        after 1000
        set redis_no_ssdb [redis 127.0.0.1 6389]
        list [$redis locatekey foo] [ $redis_no_ssdb get foo ] [ $ssdb get foo ]
    } {ssdb {} bar}

    test "Redis can read key stored in ssdb" {
        $redis get foo
    } {bar}

    test "GET Key(become hot) move from ssdb to redis" {
        list [$redis locatekey foo] [ $ssdb get foo ]
    } {redis {}}

    test "Key(become cold) move from redis to ssdb" {
        $redis dumptossdb foo
        list [$redis locatekey foo] [ $ssdb get foo ]
    } {ssdb bar}

    test "SET Key(become hot) move from ssdb to redis" {
        $redis set foo bar1
        list [$ssdb get foo] [$redis get foo]
    } {{} bar1}

    test "Redis can DEL key loaded from ssdb to redis" {
        $redis del foo
        list [$redis locatekey foo] [$redis get foo] [$ssdb get foo]
    } {none {} {}}

    test "Key(become cold) move from redis to ssdb" {
        # TODO
        $redis set foo bar
        $redis dumptossdb foo
        list [$redis locatekey foo] [ $ssdb get foo ]
    } {ssdb bar}

    test "Redis can DEL key stored in ssdb" {
        $redis del foo
        list [$redis locatekey foo] [$redis get foo] [$ssdb get foo]
    } {none {} {}}

    test "SET new key(Hot key) not store in ssdb" {
        $redis set foo bar
        list [$redis get foo] [$ssdb get foo]
    } {bar {}}

    test "GET key(Hot key) store in redis not operate ssdb" {
        set precalls [ get_total_calls "total_calls" $ssdb]
        $redis get foo
        set nowcalls [ get_total_calls "total_calls" $ssdb]
        expr $nowcalls-$precalls
    } 1

    test "GET key(not exist) not operate ssdb" {
        set precalls [ get_total_calls "total_calls" $ssdb]
        $redis get fooxxx
        set nowcalls [ get_total_calls "total_calls" $ssdb]
        expr $nowcalls-$precalls
    } 1

    catch {exec kill -9 $nossdbpid}
    catch {exec kill -9 $ssdbpid}
}
