Add 7 new test cases to planner-exec.pg.test.ts to validate additional
cost model scenarios:

1.  Low Fanout Chain - Tests FK relationships (1:1) with very selective
    filter at deepest level

- Query: invoiceLine → track → album where album title is very
  specific
- Tests: Fanout scaling when f≈1, limit propagation through low-fanout
  chain

2.  Extreme Selectivity + High Fanout - Tests fanout amplification of
    rare filters

- Query: artist → albums → tracks where tracks.milliseconds > 10M
  (very rare)
- Tests: 1 - (1-s)^f formula with s<0.01, verifies rare filters become
  likely with high fanout

3.  Deep Nesting (4 levels) - Tests compound selectivity through deep
    chain

- Query: invoiceLine → invoice → customer → employee with employee
  filter
- Tests: Compound downstream selectivity calculation, constraint
  propagation through 4 levels

4.  OR with Asymmetric Fanout - Tests parallel branches with different
    characteristics

- Query: track with OR of album (low fanout) vs invoiceLines (high
  fanout)
- Tests: Fan-in cost estimation when branches have vastly different
  fanouts

5.  Many-to-Many Junction - Tests extreme fanout through junction table

- Query: playlist → playlistTrack → track with selective
  track.composer filter
- Tests: Junction table handling, extreme fanout (~484:1), flip
  decisions with intermediate hop

6.  Empty Result Set - Tests zero-selectivity edge case

- Query: track → album → artist where artist doesn't exist
- Tests: Division by zero protection, zero-row handling, early
  termination

7.  Sparse Foreign Key - Tests NULL handling in fanout calculation

- Query: track → album with IS NOT NULL check and selective filter
- Tests: SQLite stat4 NULL exclusion logic, sparse FK cost estimation

Each test validates correlation ≥ 0.7 between estimated and actual
costs.
