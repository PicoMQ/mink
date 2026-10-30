#!/bin/sh

set -eu

cd "$(dirname "$0")"

if [ -f server.pem ]; then
  exit 0
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=mink test CA" \
  -keyout ca.key -out ca.pem
openssl req -newkey rsa:2048 -nodes -subj "/CN=mink test server" \
  -keyout server.key -out server.csr
printf 'subjectAltName=DNS:kafka,DNS:postgres,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 3650 \
  -extfile server.ext -out server.pem
cat server.key server.pem ca.pem > kafka.pem
rm ca.key ca.srl server.csr server.ext
chmod 644 ca.pem server.pem server.key kafka.pem
