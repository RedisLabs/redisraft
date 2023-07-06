# RedisRaft/Jepsen Tests on Docker

This is a `docker-compose` setup for running a RedisRaft/Jepsen test on a docker
environment.

The environment consists of a single control container and 5 cluster containers
running RedisRaft. All containers are based on Debian.

## Getting started

First, you need to generate SSH keys that will be populated and used for the
control container to access the cluster containers.

Run:

    ./genkeys.sh

This should result with a `secret/` directory created with configuration files
in it.

Next, build the images:

    docker-compose build

And start the environment:

    docker-compose up -d

## Running

To run a test:

    docker exec -w /jepsen jepsen-control \
        lein run test-all --ssh-private-key /root/.ssh/id_rsa \
            --follower-proxy \
            --time-limit 600 \
            --test-count 50 \
            --concurrency 4n \
            --nemesis partition,pause,kill,member \
            --redis-repo https://github.com/redis/redis \
            --redis-version unstable \
            --raft-repo https://github.com/redislabs/redisraft \
            --raft-version master

## Test results

The `jepsen-control` container runs a built-in web server which is exposed on
http://localhost:8080 and can be used to look at test results.

<br/><br/>
<br/><br/>


