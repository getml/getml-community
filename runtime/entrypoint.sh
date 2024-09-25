#!/usr/bin/env bash

for port in $(seq "$PORT_MIN" "$PORT_MAX"); do
    socat tcp-l:$((port + 10000)),fork,reuseaddr tcp:127.0.0.1:$port &
done

if [ -z "$1" ]; then
    /usr/local/bin/getML \
        -launch-browser=${GETML_LAUNCH_BROWSER} \
        -home-directory=${GETML_HOME_DIRECTORY} \
        -http-port=${GETML_HTTP_PORT} \
        -in-memory=${GETML_IN_MEMORY} \
        -install=${GETML_INSTALL} \
        -log=${GETML_LOG} \
        -project-directory=${GETML_PROJECT_DIRECTORY} \
        -proxy-url=${GETML_PROXY_URL} \
        -token=${GETML_TOKEN}
else
    exec "$@"
fi
