// NOTE: this file must sort after custom.test.ts; see legacy-crud.test.ts.
import {expect, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Codec} from '../../../zero-types/src/schema-value.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {
  defineQueriesWithType,
  defineQueryWithType,
} from '../../../zql/src/query/query-registry.ts';
import type {Zero} from './zero.ts';

const schema = createSchema({
  tables: [
    table('event').columns({id: string(), at: number()}).primaryKey('id'),
  ],
});
const zql = createBuilder(schema);

const argsCodec: Codec<{at: number}, {at: Date}> = {
  decode: ({at}) => ({at: new Date(at)}),
  encode: ({at}) => ({at: at.getTime()}),
};

// Bind the schema so the only difference from a plain query is the codec.
const defineQuery = defineQueryWithType<typeof schema, unknown>();
const queries = defineQueriesWithType<typeof schema>()({
  byTime: defineQuery(argsCodec, ({args}) =>
    zql.event.where('at', '=', args.at.getTime()),
  ),
});

test('codec query requests are accepted by run / preload / materialize', () => {
  // Type-only: a codec query's decoded args (a Date) are not JSON, which the
  // Zero read APIs used to reject.
  const useIt = (z: Zero<typeof schema>) => {
    void z.run(queries.byTime({at: new Date(0)}));
    z.preload(queries.byTime({at: new Date(0)}));
    z.materialize(queries.byTime({at: new Date(0)})).destroy();
  };
  expect(typeof useIt).toBe('function');
});
