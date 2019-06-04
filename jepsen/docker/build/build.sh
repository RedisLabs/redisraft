#!/bin/bash
set -e

REDIS_REPO=${REDIS_REPO:-yossigo/redis}
REDIS_VERSION=${REDIS_VERSION:-blocking-api-fix}
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
curl -L https://github.com/${REDIS_REPO}/archive/${REDIS_VERSION}.tar.gz | tar xfz - --transform='s,[^/]*,redis,'

# Avoid core dump corruption:
sed -i '/^#define HAVE_PROC_MAPS/ d' redis/src/config.h

make -C redis
make -C redisraft

DISTDIR=redisraft-$VERSION
mkdir $DISTDIR
cp redis/src/redis-server $DISTDIR
cp redis/src/redis-cli $DISTDIR
cp redisraft/redisraft.so $DISTDIR

tar cvfz /dist/redisraft-$VERSION-linux-amd64.tar.gz $DISTDIR
