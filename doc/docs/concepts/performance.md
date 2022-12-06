---
title: Performance
slug: /concepts/performance
---

## Performance Intuitions

Your intuition should be that Replicache has "memory-fast" performance for common operations. Here are some rough rules of thumb that should serve you well.

| Operation                              | Expectation            |
| -------------------------------------- | ---------------------- |
| Read 1 value                           | < 1ms                  |
| Read (scan) keys in order              | > 500 MB/s             |
| Write 1 value and commit tx            | < 1ms                  |
| Write 1KB values in bulk and commit tx | > 90 MB/s              |
| Start from disk                        | First 100KB in < 150ms |

## Typical Workload

Here are some axes along which you could measure the workload that Replicache is designed to work with. These are not hard constraints, they give ranges in which we would expect Replicache to work without caveats. If you want to operate outside of these ranges, it's probably a good idea to talk to us.

| Axis                                 | Expectation                            |
| ------------------------------------ | -------------------------------------- |
| Total data size                      | < 64MB per cache                       |
| Typical key-value size               | 100 bytes - 10KB                       |
| Max key-value size                   | < 1MB (see also [blobs](/howto/blobs)) |
| Average push-pull round trip latency | 100's of ms                            |
| Number of indexes                    | < 5                                    |

## Specific Performance Metrics

Below find some specific performance metrics that Replicache meets or exceeds. We track these metrics (and more) as part of our continuous integration strategy, measuring them on stock desktop hardware (4-core Xeon from 2018-ish, 16GB RAM) for every change to the codebase.

Note that these are microbenchmarks with very specific payloads. Actual performance will vary. If you experience worse performance than suggested below we'd likely consider it a bug, so please contact us.
<br/><br/>

### Scan: 650MB/s

---

This is the rate at which key-values can be iterated in key order.
<br/><br/>

### Reactive Loop w/16MB cache: 3ms @p50, 7ms @p95

### Reactive Loop w/64MB cache: 3.5ms @p50, 7ms @p95

---

The reactive loop latency is the time it takes to write new data, notify all subscribers of the change, and for them to read the new data out. Assumptions: there are 100 open subscriptions 5 of which are dirty, and each of these 5 reads 10KB of data.
<br/><br/>

### Populate 1MB w/0 indexes: 90MB/s

### Populate 1MB w/1 indexes: 45MB/s

### Populate 1MB w/2 indexes: 30MB/s

---

This measures the rate at which callers can write 1MB's worth of 1KB key-values.
<br/><br/>

### Startup: 100KB in < 150ms @p95

---

This measures the p95 time to read the first 100KB of data from disk at Replicache startup.
