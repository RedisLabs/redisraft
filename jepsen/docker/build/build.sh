#!/bin/bash
set -e

if [ -z "$VERSION" ]; then
    echo "Missing VERSION environment variable!"
    exit 1
fi

git clone --recursive https://github.com/yossigo/redisraft
git clone https://github.com/antirez/redis

make -C redis
make -C redisraft

DISTDIR=redisraft-$VERSION
mkdir $DISTDIR
cp redis/src/redis-server $DISTDIR
cp redis/src/redis-cli $DISTDIR
cp redisraft/redisraft.so $DISTDIR

tar cvfz /dist/redisraft-$VERSION-linux-amd64.tar.gz $DISTDIR
