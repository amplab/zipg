#!/usr/bin/env bash
sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/succinct-config.sh"

if [ "$SUCCINCT_LOG_PATH" = "" ]; then
	SUCCINCT_LOG_PATH="$SUCCINCT_HOME/log"
fi

FILE="${SUCCINCT_LOG_PATH}/handler.log"

while : ;do
  [[ -f "$FILE" ]] && grep -q "Handler started" "$FILE" && echo "Handler started!" && break
  sleep 5
done