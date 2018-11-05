#!/bin/bash
VERSION=1.0
if [ ! -f redisraft.h ]; then
    echo "Run this script from the top level directory!"
    exit 1
fi

docker build -t redisraft-build ./jepsen/docker/build

mkdir -p jepsen/docker/dist
make cleanall
docker run --rm -v `pwd`:/src -e VERSION=$VERSION -v `pwd`/jepsen/docker/dist:/dist redisraft-build

