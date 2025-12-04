// This file outputs the defined queries from wide and deep queries.

import {defineQueriesWithType} from '../../../zql/src/query/query-registry.ts';
import {queryDeep} from './zero-stress-queries-deep-test.ts';
import {queryWide} from './zero-stress-queries-wide-test.ts';
import type {zeroStressSchema} from './zero-stress-schema-test.ts';

const defineQueries = defineQueriesWithType<typeof zeroStressSchema>();

const queries = defineQueries({
  wide: queryWide,
  deep: queryDeep,
});

// this is testing .d.ts generation for queries
export {queries};
