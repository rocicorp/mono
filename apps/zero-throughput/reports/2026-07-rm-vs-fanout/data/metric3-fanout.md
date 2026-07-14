# Metric (3): RM->VS fan-out (subscriber-sim, feed-append, writer 2000 changes/s on 4849)

## Fan-out to FAST subscribers is linear (no wire bottleneck when subscribers keep up)
| K | per-subscriber changes/s | total delivered/s |
|--:|--:|--:|
| 1 | 2000 | 2000 |
| 2 | 2000 | 4000 |
| 4 | 2000 | 8000 |
| 8 | 1976 | 15,809 |
(K=1 raw wire ceiling measured separately ~18,900 changes/s.)

## One SLOW subscriber stalls ALL healthy subscribers (flow-control coupling)
K=4, 1 slow subscriber (per-message ack delay), padding=1 (default). Fast-subscriber avg:
| slow delay | fast subscribers changes/s | collapse vs 2000 |
|--:|--:|--:|
| 1 ms  | 768 | 2.6x |
| 2 ms  | 48  | 42x  |
| 5 ms  | 48-96 | ~30x |
| 10 ms | 384 | 5x |
| 20 ms | 1430 | 1.4x |
| 50 ms | 2869 (catching up) | ~none |

Non-monotonic: a MILDLY slow subscriber (2-5 ms/msg) is the WORST — it stays in the
"pending" set at every 64 KiB flow-control checkpoint, so the RM waits the full
consensus padding (up to 1s, on a 1s progress-monitor tick) every checkpoint, throttling
the WHOLE stream. A very slow subscriber falls so far behind it stops mattering to the
majority and fast subscribers run free.

=> This is the "adding a (struggling) view-syncer degrades throughput for everyone"
   mechanism, isolated. The global consensus gate couples all subscribers to the
   slowest-in-majority. A view-syncer that briefly lags (GC pause, big advance, catchup)
   drags the entire fan-out — and thus every other view-syncer and the RM's pipeline
   advancement — down with it.

## Padding knob gives only PARTIAL relief (the 1s tick still gates)
K=4, 1 slow subscriber, fast-subscriber avg changes/s (writer=2000):
| slow delay | padding=1 (default) | padding=0 (release on majority) |
|--:|--:|--:|
| 2 ms  | 48   | 412  |
| 5 ms  | 48-96 | 97  |
| 20 ms | 1430 | 2116 |

padding=0 helps at 2ms (48->412) but fast subscribers are STILL throttled ~5x. Root cause:
`Forwarder` only runs `checkProgress` on a **1s progress-monitor tick** (forwarder.ts:91),
and `Broadcast.done` resolves early only when a majority has acked AND a tick fires. Between
ticks the global broadcast is stuck waiting for the slow subscriber. So no padding value
fully decouples fast subscribers — the mechanism is a GLOBAL gate, not per-subscriber.

FIX DIRECTION (design): per-subscriber bounded send queues; the RM never blocks the whole
stream on any one subscriber. A lagging subscriber's queue fills and it falls back to
changelog catchup (RMv2-aligned), while fast subscribers proceed at full rate.
