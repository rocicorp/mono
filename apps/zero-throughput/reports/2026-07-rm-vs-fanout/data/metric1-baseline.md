# Metric (1): sustainable hot-model writes/s vs sync workers (single task)
relational hot, 50 users, 3 queries/user, 50 rows/query, 45s runs, 4-vCPU box.

| sync workers | best sustainable wps | p99 client lag @ best | maxSeqLag | lagSlope |
|---:|---:|---:|---:|---:|
| 1 | 3 | 1124 ms | 5 | 0.00 |
| 2 | 6 | 1131 ms | 9 | -0.04 |
| 4 | 6 | 1110 ms | 9 | -0.02 |

Scaling: 1->2 workers = 2.0x (3->6). 2->4 workers = 1.0x (6->6, NO gain).
On 4 vCPUs the box is CPU-saturated at 2 sync workers (change-streamer + replicator +
2 syncers + writer + PG already fill 4 cores), so adding workers 3-4 does nothing.
This reproduces "adding view-syncers does nothing for advancement" at the sync-worker
level; here the cause is CPU saturation. The subscriber-sim isolates whether RM->VS
comms ALSO cap it independent of CPU.
