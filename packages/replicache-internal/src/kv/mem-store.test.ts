import {runAll} from './store-test-util';
import {MemStore} from './mem-store';

runAll('memstore', () => new MemStore());
