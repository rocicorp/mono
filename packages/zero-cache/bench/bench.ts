import 'dotenv/config';
import {bench} from './benchmark.js';

bench({dbFile: '/tmp/bench/zbugs-sync-replica.db'});
