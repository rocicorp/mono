import {runTests} from '../all-integration.js';
import {newSqliteZero} from './new-zql-lite-zero.js';
import {test} from 'vitest';

runTests(newSqliteZero);

test('no tests', () => {});
