#!/bin/bash

# Generate some test certificates which are used by the regression test suite:
#
#   tests/tls/ca.{crt,key}          Self signed CA certificate.
#   tests/tls/redis.{crt,key}       A certificate with no key usage/policy restrictions.
#   tests/tls/client.{crt,key}      A certificate restricted for SSL client usage.
#   tests/tls/server.{crt,key}      A certificate restricted for SSL server usage.
#   tests/tls/redis.dh              DH Params file.

generate_cert() {
    local dir="$1"
    local name="$2"
    local cn="$3"
    local opts="$4"

    local keyfile="${dir}/${name}.key"
    local certfile="${dir}/${name}.crt"

    [ -f $keyfile ] || openssl genrsa -out $keyfile 2048
    openssl req \
        -new -sha256 \
        -subj "/O=Redis Test/CN=$cn" \
        -key $keyfile | \
        openssl x509 \
            -req -sha256 \
            -CA "${dir}/ca.crt" \
            -CAkey "${dir}/ca.key" \
            -CAserial "${dir}/ca.txt" \
            -CAcreateserial \
            -days 365 \
            $opts \
            -out $certfile
}

DIR="${1:-tests/tls}"

mkdir -p "$DIR"

[ -f "$DIR/ca.key" ] || openssl genrsa -out "${DIR}/ca.key" 4096
openssl req \
    -x509 -new -nodes -sha256 \
    -key "${DIR}/ca.key" \
    -days 3650 \
    -subj '/O=Redis Test/CN=Certificate Authority' \
    -out "${DIR}/ca.crt"

cat > "${DIR}/openssl.cnf" <<_END_
[ server_cert ]
keyUsage = digitalSignature, keyEncipherment
nsCertType = server

[ client_cert ]
keyUsage = digitalSignature, keyEncipherment
nsCertType = client
_END_

generate_cert "$DIR" server "Server-only" "-extfile ${DIR}/openssl.cnf -extensions server_cert"
generate_cert "$DIR" client "Client-only" "-extfile ${DIR}/openssl.cnf -extensions client_cert"
generate_cert "$DIR" redis "Generic-cert"

[ -f $DIR/redis.dh ] || openssl dhparam -out $DIR/redis.dh 2048
