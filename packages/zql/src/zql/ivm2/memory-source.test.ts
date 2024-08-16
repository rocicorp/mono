import {Ordering} from '../ast2/ast.js';
import {MemorySource} from './memory-source.js';
import {runCases} from './test/source-cases.js';

runCases((order: Ordering) => new MemorySource(order));
