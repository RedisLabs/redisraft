#!/bin/bash
if [ -z "$AUTHORIZED_KEYS" ]; then
  echo "ERROR: No AUTHORIZED_KEYS environment was setup!"
  exit 1
fi

mkdir -p /root/.ssh
chmod 0700 /root/.ssh
echo "${AUTHORIZED_KEYS}" > /root/.ssh/authorized_keys
chmod 0600 /root/.ssh/authorized_keys

if [ -n "$PRIVATE_KEY" ]; then
  echo "$PRIVATE_KEY" | sed 's/\\n/\
/g' > /root/.ssh/id_rsa
  chmod 0600 /root/.ssh/id_rsa
fi

if [ -n "$KNOWN_HOSTS" ]; then
  echo "$KNOWN_HOSTS" | sed 's/\\n/\
/g' > /root/.ssh/known_hosts
  chmod 0600 /root/.ssh/known_hosts
fi

exec /usr/sbin/sshd -D
