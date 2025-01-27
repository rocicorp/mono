import {runAll} from './store-test-util.ts';
import {TestMemStore} from './test-mem-store.ts';

runAll('TestMemStore', () => new TestMemStore());
