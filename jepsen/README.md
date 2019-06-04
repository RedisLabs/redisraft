# Jepsen Test

This is a preliminary effort towards using Jepsen to test Redis Raft's
consistency.

You can find here the Jepsen test code to handle Redis Raft setup and run
linearizability tests, as well a basic Docker Compose environment.

# Building

First, you'll need to build a tarball that contains a compatible version of
Redis and the Redis Raft, compiled for Debian.

To do that, run:

    ./jepsen/docker/build_dist.sh

This should result with a `jepsen/docker/dist` directory being created with a
single tarball.

# Docker Environment

To start Jepsen:

    cd jepsen/docker
    ./up.sh

This will launch Jepsen's built-in web server on `http://localhost:8080`, but do
nothing else.

# Running a test

To start an actual test, use a command such as:

    docker exec -ti jepsen-control bash
    cd /jepsen
    lein run test --time-limit 60 --concurrency 50 --ssh-private-key /root/.ssh/id_rsa

# Work In Progress

This is very preliminary and should be considered work in progress.
Some issues on our wish list:

- [] Implement support for catching `-MOVED` and redirecting requests to the
  leader. This should be the preferred way of working with Redis Raft.
- [] Add an `MULTI/EXEC/WATCH` based implementation of CAS.

