#!/bin/bash
colorize() {
    local colstr
    case $1 in 
        red) color="31m" ;;
        green) color="32m" ;;
        yellow) color="33m" ;;
        blue) color="34m" ;;
        magenta) color="35m" ;;
        cyan) color="36m" ;;
    esac

    while read line;
    do
        echo -e "\033[${color}${line}\033[0m"
    done
}

../redis/src/redis-server --port 5001 --loadmodule `pwd`/rafter.so 1 2:localhost:5002 3:localhost:5003 2>&1 | colorize yellow&
../redis/src/redis-server --port 5002 --loadmodule `pwd`/rafter.so 2 1:localhost:5001 3:localhost:5003 2>&1 | colorize magenta&
../redis/src/redis-server --port 5003 --loadmodule `pwd`/rafter.so 3 1:localhost:5001 2:localhost:5002 2>&1 | colorize cyan&
read
