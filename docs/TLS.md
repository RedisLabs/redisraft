RedisRaft TLS Support
=====================

RedisRaft's TLS support mirrors Redis's native TLS support.  

One runs the underlying redis server in the same manner that one would run a redis server with TLS

```
redis-server --tls-port <por> --port 0 --tls-cert-file <file> --tls-key-file <file> --tls-ca-cert-file <file> 
```

In addition, one has to pass the `tls-enabled yes` option to the redisraft module

```
--loadmodule ./redisraft.so <other options> tls-enabled yes
```

RedisRaft is smart enough to figure out which files to use for the local cert/key/ca by querying the underlying Redis server.  However, if a user wants to use different files for the client aspects vs server aspects, one can specify RedisRaft module options as well

```asm
tls-ca-cert
tls-cert
tls-key
```