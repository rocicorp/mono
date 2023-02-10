# Load Testing Replidraw

This is a simple load test based on k6 (k6.io).

To run:

1. [Install k6](https://k6.io/docs/getting-started/installation/)
2. `k6 run --vus=20 --duration=10s -e ROOM_ID=r2 test.js`

The `--vus` flag controls the number of "virtual user" that will be simulated.
