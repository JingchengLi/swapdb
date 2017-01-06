# set client [redis 127.0.0.1 8888]
# dict set srv "client" $client
# proc r {args} {
    # set ssdb [redis 127.0.0.1 8888]
    # $ssdb {*}$args
# }

# Disable this function for ssdb
proc assert_encoding {enc key} {

}

