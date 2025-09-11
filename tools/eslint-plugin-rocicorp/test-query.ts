// Test file to verify the no-unhandled-query rule
import type { Query } from '../../packages/zql/src/query/query';

declare const q: Query<any, any, any>;

// These should trigger the rule (unhandled queries)
q.limit(1); // ERROR: Query not handled
q.where('field', 'value'); // ERROR: Query not handled

// These should NOT trigger the rule (properly handled queries)
const result1 = q.limit(1); // OK: assigned to variable
await q.limit(1); // OK: awaited
return q.limit(1); // OK: returned

function test() {
  q.limit(1).run(); // OK: chained with run()
  q.limit(1).materialize(); // OK: chained with materialize()
  
  const arr = [q.limit(1)]; // OK: in array
  const obj = { query: q.limit(1) }; // OK: in object
  
  someFunction(q.limit(1)); // OK: passed as argument
}