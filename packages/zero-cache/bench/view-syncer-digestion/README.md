# View-Syncer Digestion Benchmark

This benchmark isolates the part of view-syncer catchup that happens after the
replica has already accepted upstream changes. Each view syncer starts from the
same snapshot, the benchmark applies a backlog, then every view syncer advances
its snapshot diff.

```
replica changeLog
       |
       v
  SnapshotDiff  -> active query tables -> pipeline/CVR work
       |
       `-------> unobserved table rows should not be materialized
```

The benchmark reports the old diff behavior and the filtered diff behavior in
one run. The delta is expected to be large when the backlog mostly touches
tables that no active query reads, and close to zero when every changed row is
part of the active query table set.

Run:

```sh
npm --workspace=zero-cache run perf:vs-digestion
```

Useful knobs:

```sh
ZERO_VS_DIGESTION_VIEW_SYNCERS=16
ZERO_VS_DIGESTION_TX=1000
ZERO_VS_DIGESTION_ROWS_PER_TX=10
ZERO_VS_DIGESTION_OUT=/tmp/vs-digestion.json
```
