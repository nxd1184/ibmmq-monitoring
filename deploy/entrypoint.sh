#!/bin/sh
# Starts the app. JAVA_TOOL_OPTIONS and JAVA_OPTS are honoured.
# MQMONITOR_LEGACY_TLS=true re-enables old TLS protocols and cipher suites (see java.security.legacy-tls).
set -eu

if [ "${MQMONITOR_LEGACY_TLS:-false}" = "true" ]; then
  JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Djava.security.properties=/app/deploy/java.security.legacy-tls"
  export JAVA_TOOL_OPTIONS
fi

# JAVA_OPTS is split on purpose, so it can hold several options.
# shellcheck disable=SC2086
exec java ${JAVA_OPTS:-} -jar /app/app.jar "$@"
