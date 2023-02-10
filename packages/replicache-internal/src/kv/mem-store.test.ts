import {runAll} from './store-test-util.js';
import {MemStore} from './mem-store.js';

runAll('memstore', () => new MemStore());
