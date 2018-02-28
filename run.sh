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

../redis/src/redis-server --port 5001 --logfile redis1.log --loadmodule `pwd`/redisraft.so id=1 node=2,localhost:5002 node=3,localhost:5003  &
../redis/src/redis-server --port 5002 --logfile redis2.log --loadmodule `pwd`/redisraft.so id=2 node=1,localhost:5001 node=3,localhost:5003 &
../redis/src/redis-server --port 5003 --logfile redis3.log --loadmodule `pwd`/redisraft.so id=3 node=1,localhost:5001 node=2,localhost:5002 &
read
