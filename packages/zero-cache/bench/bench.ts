import '@dotenvx/dotenvx/config';
import {bench} from './benchmark.ts';

bench({dbFile: '/tmp/bench/zbugs-sync-replica.db'});
