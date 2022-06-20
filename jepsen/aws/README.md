# RedisRaft/Jepsen Tests on AWS

This is a Terraform script to launch a Jepsen test environment on AWS.
It launches and configures 5 cluster test nodes and one control node.

All nodes run Debian stretch.

## Getting started

Set up your terraform environment and run:

    terraform -var aws_key_pair_name=<your-key-pair> apply

## Connecting

Once launched, connect to the control node:

    ssh admin@<control address>

## Installing Jepsen

Clone the Jepsen Redis test. You may need the auto generated SSH key file
authorized to do that in GitHub.

    git clone https://github.com/redislabs/jepsen-redisraft jepsen
    cd jepsen
    lein install

## Running a test

To run a test:

    lein run test-all --ssh-private-key ~/.ssh/id_rsa \
        --nodes-file ~/nodes.txt \
        --follower-proxy \
        --time-limit 600 \
        --test-count 50 \
        --concurrency 4n \
        --redis-repo https://github.com/redis/redis \
        --redis-version unstable \
        --raft-repo https://github.com/redislabs/redisraft \
        --raft-version master
