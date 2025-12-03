// This file outputs the defined queries from wide and deep queries.

import {defineQueries} from '../../../zql/src/query/query-registry.ts';
import {queryDeep} from './zero-stress-queries-deep-test.ts';
import {queryWide} from './zero-stress-queries-wide-test.ts';

const queries = defineQueries({
  wide: queryWide,
  deep: queryDeep,
});

// this is testing .d.ts generation for queries
export {queries};
