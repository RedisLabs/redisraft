# RedisRaft TLS Support

RedisRaft's TLS support mirrors Redis's native TLS support.  

One runs the underlying redis server in the same manner that one would run a redis server with TLS

```
redis-server --tls-port <por> --port 0 --tls-cert-file <file> --tls-key-file <file> --tls-ca-cert-file <file> 
```

In addition, one has to pass the `tls-enabled yes` option to the redisraft module

```
--loadmodule ./redisraft.so <other options> tls-enabled yes
```

## Configuring RedisRaft TLS support

RedisRaft can be configured by reusing the Redis server's TLS configuration, or by configuring the RedisRaft module directly. In general, we expect people will configure the Redis server. 

### Configuring RedisRaft via Redis.

| Config Key Name          | Meaning                                                                                                   |
|--------------------------|-----------------------------------------------------------------------------------------------------------|
| tls-ca-cert-file         | File Containing the CA Certificate                                                                        |
| tls-key-file             | File Containing the PEM encoded private key file                                                          |
| tls-client-key-file      | File Containing the PEM encoded private key file for client connections (overrides value of tls-key-file) |
| tls-cert-file            | File Containing the PEM encoded public signed CERT                                                        |
| tls-client-cert-file     | File Containing the PEM encoded public signed CERT (overrides tls-cert-file)                              |                                                                                                          |
| tls-key-file-pass        | String containing password to decrypt the private key, if encrypted                                       |
| tls-client-key-file-pass | String containing password to decrypt the private key if using client key field                           | 

### Configuring RedisRaft

| Redis Module Config Key Name | Maps to Redis Configuration |
|------------------------------|-----------------------------|
| tls-ca-cert                  | tls-ca-cert-file            |
| tls-key                      | tls-key-file                |
| tls-cert                     | tls-cert-file               |
| tls-key-pass                 | tls-key-file-pass           |

One thing to note, if one configures RedisRaft via the module configuration, one should ensure that one configures all required fields via the module configuration options.