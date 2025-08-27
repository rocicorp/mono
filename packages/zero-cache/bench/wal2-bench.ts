import '../../shared/src/dotenv.ts';
import {walBenchmark} from './wal-benchmark.ts';

walBenchmark({
  dbFile: '/tmp/bench/zbugs-sync-replica.db',
  mode: 'WAL2',
  runs: 100,
  modify: 1000,
});
