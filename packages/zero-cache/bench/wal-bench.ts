import '../../shared/src/dotenv.ts';
import {walBenchmark} from './wal-benchmark.ts';

walBenchmark({
  dbFile: '/tmp/bench/zbugs-sync-replica.db',
  mode: 'WAL',
  runs: 100,
  modify: 1000,
});
