import 'dotenv/config';
import {bench} from './benchmark.ts';

bench({dbFile: '/tmp/bench/zbugs-sync-replica.db'});
