#!/bin/bash
set -e

mkdir -p /root/.ssh
chmod 0700 /root/.ssh

if [ -n "$PRIVATE_KEY" ]; then
  echo "$PRIVATE_KEY" | sed 's/\\n/\
/g' > /root/.ssh/id_rsa
  chmod 0600 /root/.ssh/id_rsa
fi

echo "$KNOWN_HOSTS" | sed 's/\\n/\
/g' > /root/.ssh/known_hosts
chmod 0600 /root/.ssh/known_hosts

cd /jepsen
lein run serve
