proc check_python_result {pattern log} {
    catch {[exec grep $pattern $log]} err
    if {[string match "*child process*" $err]} {
        exec cat $log
        fail "$pattern not found in $log"
    }
}

start_server {tags {"ssdb"} } {
    test "test_mthreads by python" {
        set port [srv port]
        set mthreads [regsub -all {stdout} [ srv 0 stdout ] mthreads]
        set pid [ exec python python/test_mthreads.py $port > $mthreads 2>> $mthreads]
        check_python_result "OK" $mthreads
        r ping
    } {PONG}
}

start_server {tags {"ssdb"} } {
    test "test_pipeline by python" {
        set port [srv port]
        set pipeline [regsub -all {stdout} [ srv 0 stdout ] pipeline]
        set pid [ exec python python/test_pipeline.py $port > $pipeline 2>> $pipeline]
        check_python_result "OK" $pipeline
        r ping
    } {PONG}
}

start_server {tags {"ssdb"} } {
    test "test_midstate by python" {
        set port [srv port]
        set midstate [regsub -all {stdout} [ srv 0 stdout ] midstate]
        set pid [ exec python python/test_midstate.py $port > $midstate 2>> $midstate]
        check_python_result "OK" $midstate
        r ping
    } {PONG}
}
