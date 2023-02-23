import {runAll} from './store-test-util.js';
import {TestMemStore} from './test-mem-store.js';

runAll('TestMemStore', () => new TestMemStore());
