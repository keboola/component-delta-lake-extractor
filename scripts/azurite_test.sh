#!/bin/sh
# Runs tests/test_azurite_port.py: a real delta_scan against Azurite on a non-default port over HTTPS.
# Usage: scripts/azurite_test.sh <component-image>
set -e

IMAGE="$1"
NETWORK=azurite-port-test
HOST=azurite
PORT=10010
ACCOUNT=kbcportacct
WORKDIR=$(mktemp -d)

cleanup() {
    docker rm -f "$HOST" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

# Throwaway CA and server certificate: delta-kernel only allows HTTPS for Azure endpoints.
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORKDIR/ca.key" -out "$WORKDIR/ca.crt" -days 1 \
    -subj "/CN=azurite-test-ca" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout "$WORKDIR/server.key" -out "$WORKDIR/server.csr" \
    -subj "/CN=$HOST" 2>/dev/null
printf "subjectAltName=DNS:%s\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n" "$HOST" > "$WORKDIR/ext.cnf"
openssl x509 -req -in "$WORKDIR/server.csr" -CA "$WORKDIR/ca.crt" -CAkey "$WORKDIR/ca.key" -CAcreateserial \
    -out "$WORKDIR/server.crt" -days 1 -extfile "$WORKDIR/ext.cnf" 2>/dev/null
chmod 644 "$WORKDIR"/*
ACCOUNT_KEY=$(openssl rand -base64 64 | tr -d '\n')

docker network create "$NETWORK" >/dev/null
docker run -d --name "$HOST" --network "$NETWORK" -v "$WORKDIR:/certs:ro" \
    -e AZURITE_ACCOUNTS="$ACCOUNT:$ACCOUNT_KEY" \
    mcr.microsoft.com/azure-storage/azurite:3.35.0 \
    azurite-blob --blobHost 0.0.0.0 --blobPort "$PORT" --disableProductStyleUrl --skipApiVersionCheck --loose \
    --cert /certs/server.crt --key /certs/server.key >/dev/null

for _ in $(seq 1 30); do
    docker logs "$HOST" 2>&1 | grep -q "successfully listens" && break
    sleep 1
done

docker run --rm --network "$NETWORK" -v "$WORKDIR/ca.crt:/usr/local/share/ca-certificates/azurite-test-ca.crt:ro" \
    -e AZURITE_BLOB_ENDPOINT="https://$HOST:$PORT/$ACCOUNT" \
    -e AZURITE_ACCOUNT="$ACCOUNT" \
    -e AZURITE_ACCOUNT_KEY="$ACCOUNT_KEY" \
    -e REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    "$IMAGE" \
    sh -c "update-ca-certificates >/dev/null 2>&1 && python -m unittest tests.test_azurite_port -v"
