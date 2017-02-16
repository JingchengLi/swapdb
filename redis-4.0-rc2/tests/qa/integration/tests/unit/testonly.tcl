start_server {tags {"testonly"}} {
    # Enable the AOF
    set r [redis 127.0.0.1 6379]
    puts "[$r dbsize]"
}
