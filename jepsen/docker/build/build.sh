#!/bin/bash
set -e

REDIS_VERSION=${REDIS_VERSION:-5.0.0}
if [ -z "$VERSION" ]; then
    echo "Missing VERSION environment variable!"
    exit 1
fi

if [ ! -f /src/redisraft.h ]; then
    echo "Redis Raft module source not found!"
    exit 1
fi

mkdir -p /work/redisraft
cd /work/redisraft
(cd /src ; tar cf - * --exclude jepsen) | tar xf -

cd /work
curl -L https://github.com/antirez/redis/archive/${REDIS_VERSION}.tar.gz | tar xfz -
mv redis-${REDIS_VERSION} redis

make -C redis
make -C redisraft

DISTDIR=redisraft-$VERSION
mkdir $DISTDIR
cp redis/src/redis-server $DISTDIR
cp redis/src/redis-cli $DISTDIR
cp redisraft/redisraft.so $DISTDIR

tar cvfz /dist/redisraft-$VERSION-linux-amd64.tar.gz $DISTDIR
