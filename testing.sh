#!/bin/bash
#
# testing.sh
#

PORT=$((5000 + RANDOM % 1000))
TIMEOUT=5

# Start dbserver in background, and send "quit" after a timeout
(
  sleep $TIMEOUT
  echo "quit"
) | ./dbserver $PORT &

# Record the server's PID
SERVER_PID=$!

sleep 0.5

# Simple tests
echo "==> Testing single commands..."
./dbtest --port=$PORT --set=foo hello
./dbtest --port=$PORT --get=foo
./dbtest --port=$PORT --delete=foo

# Concurrency tests
echo "==> Testing concurrency with 5 threads & 50 requests..."
./dbtest --port=$PORT --threads=5 --count=50
echo "==> Testing concurrency with 10 threads & 100 requests..."
./dbtest --port=$PORT --threads=10 --count=100
echo "==> Testing concurrency with 20 threads & 200 requests..."
./dbtest --port=$PORT --threads=20 --count=200

# Wait for the server to exit
wait $SERVER_PID
STATUS=$?

if [ $STATUS -ne 0 ]; then
  echo "FAILED: dbserver exited with code $STATUS"
  exit 1
else
  echo "PASSED: All tests completed successfully."
fi
