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

valgrind --suppressions=valgrind.sup --leak-check=full --show-reachable=yes --log-file=vg1.log ../redis/src/redis-server --port 5001 --loadmodule `pwd`/redisraft.so id=1 node=2,localhost:5002 node=3,localhost:5003 2>&1 | colorize yellow&
valgrind --suppressions=valgrind.sup --leak-check=full --show-reachable=yes --log-file=vg2.log ../redis/src/redis-server --port 5002 --loadmodule `pwd`/redisraft.so id=2 node=1,localhost:5001 node=3,localhost:5003 2>&1 | colorize magenta&
valgrind --suppressions=valgrind.sup --leak-check=full --show-reachable=yes --log-file=vg3.log ../redis/src/redis-server --port 5003 --loadmodule `pwd`/redisraft.so id=3 node=1,localhost:5001 node=2,localhost:5002 2>&1 | colorize cyan&
read
