import '../../shared/src/dotenv.ts';
import {bench} from './benchmark.ts';

bench({dbFile: '/tmp/bench/zbugs-sync-replica.db'});
