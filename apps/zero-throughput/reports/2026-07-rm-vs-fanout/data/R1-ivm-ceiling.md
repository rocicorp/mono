# R1: Pure-IVM ceiling (relational hot, 300 views = 100 users x 3 queries), single-threaded
One iteration = ONE logical write (insert relActivity + edit relAccount + edit relOrg),
advancing ALL 300 hot views (every view spans the single hot org).

| backend | mode | ms/write | => logical writes/s |
|---|---|---|---|
| zqlite (prod replica store) | push only | 192.17 | 5.20 |
| zqlite | push + flush views | 203.43 | 4.92 |
| memory | push only | 70.26 | 14.23 |
| memory | push + flush views | 73.69 | 13.57 |

Takeaway: hot-model throughput is IVM-bound at ~5 writes/s/core (zqlite). Flush adds ~5%.
memory backend ~2.7x faster than zqlite => SQLite read/scan cost per advance is significant.
This is a CEILING: no zero-cache, CVR, WebSocket, replication, or client-group overhead.
