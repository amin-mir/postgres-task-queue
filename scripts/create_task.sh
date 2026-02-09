#!/bin/sh

COUNT="${1:-1}"

case "$COUNT" in
  ''|*[!0-9]*)
    echo "usage: $0 [number_of_requests]"
    exit 1
    ;;
esac

URL="http://localhost:8080/tasks"

i=1
while [ "$i" -le "$COUNT" ]; do
  echo "Sending request $i/$COUNT"

  curl -s -X POST "$URL" \
    -H "Content-Type: application/json" \
    -d '{
      "name": "example-task",
      "type": "run_query",
      "payload": {
        "query": "SELECT * FROM users",
        "limit": "10"
      }
    }'

  echo
  i=$((i + 1))
done
