# RedisRaft TLS Support

RedisRaft's TLS support mirrors Redis's native TLS support.  

One runs the underlying redis server in the same manner that one would run a redis server with TLS

```
redis-server --tls-port <por> --port 0 --tls-cert-file <file> --tls-key-file <file> --tls-ca-cert-file <file> --tls-ca-cert-dir <dir> 
```

In addition, one has to pass the `tls-enabled yes` option to the redisraft module

```
--loadmodule ./redisraft.so <other options> --raft.tls-enabled yes
```

## Configuring RedisRaft TLS support

RedisRaft is configured by reusing the Redis server's TLS configuration. 

### Configuring RedisRaft via Redis.

| Config Key Name          | Meaning                                                                                                   |
|--------------------------|-----------------------------------------------------------------------------------------------------------|
| tls-ca-cert-file         | File Containing the CA Certificate                                                                        |
| tls-ca-cert-dir          | Directory Containing the CA Certificate                                                                   |
| tls-key-file             | File Containing the PEM encoded private key file                                                          |
| tls-client-key-file      | File Containing the PEM encoded private key file for client connections (overrides value of tls-key-file) |
| tls-cert-file            | File Containing the PEM encoded public signed CERT                                                        |
| tls-client-cert-file     | File Containing the PEM encoded public signed CERT (overrides tls-cert-file)                              |                                                                                                          |
| tls-key-file-pass        | String containing password to decrypt the private key, if encrypted                                       |
| tls-client-key-file-pass | String containing password to decrypt the private key if using client key field                           | 
