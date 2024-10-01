import 'dotenv/config';
import {walBenchmark} from './wal-benchmark.js';

walBenchmark({
  dbFile: '/tmp/bench/zbugs-sync-replica.db',
  mode: 'WAL',
  runs: 100,
  modify: 1000,
});
