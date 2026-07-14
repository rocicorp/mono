# R2: RM-side SQLite apply + change-log ceiling (combined mode, 1KB payload), single-threaded
"change" = one replica row write + one change-log append + replicationState bump. NO IVM.

| logicalTxRows | sqliteTxRows (fold) | µs/change | => changes/s |
|---|---|---|---|
| 1   | 1     | 47.53 | 21,040 |
| 1   | 1000  | 14.57 | 68,640 |
| 1   | 10000 | 15.45 | 64,725 |
| 100 | 100   | 11.33 | 88,260 |
| 100 | 1000  | 10.88 | 91,910 |
| 100 | 10000 | 10.57 | 94,610 |

Takeaway: the RM replica-apply + changelog path sustains ~21k changes/s even with NO folding
(1 SQLite tx per change), and ~90k/s when upstream txns are folded into larger SQLite commits.
This is 3-4 ORDERS OF MAGNITUDE above the ~5 logical-writes/s (~15 changes/s) IVM ceiling (R1).
=> The bottleneck is unambiguously VS-side per-client-group IVM advancement fan-out,
   NOT RM replication/apply/changelog. Batching SQLite commits (sqliteTxRows) ~3x's the apply ceiling.
