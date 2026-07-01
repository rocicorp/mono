// oxlint-disable require-await
import {assert, expect, test} from 'vitest';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import type {Codec} from '../../zero-types/src/schema-value.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {Transaction} from '../../zql/src/mutate/custom.ts';
import {
  defineMutators,
  getMutator,
} from '../../zql/src/mutate/mutator-registry.ts';
import {defineMutator} from '../../zql/src/mutate/mutator.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import {
  defineQueries,
  mustGetQuery,
} from '../../zql/src/query/query-registry.ts';
import {defineQuery} from '../../zql/src/query/query-registry.ts';
import {handleQueryRequest} from './queries/process-queries.ts';

const schema = createSchema({
  tables: [
    table('event').columns({id: string(), at: number()}).primaryKey('id'),
  ],
});
const builder = createBuilder(schema);

// Decoded args expose `at` as a Date; the encoded (wire) form uses epoch millis.
type DecodedArgs = {at: Date};
type EncodedArgs = {at: number};
const argsCodec: Codec<EncodedArgs, DecodedArgs> = {
  decode: ({at}) => ({at: new Date(at)}),
  encode: ({at}) => ({at: at.getTime()}),
};

test('server query handler decodes codec args from the wire', async () => {
  let received: DecodedArgs | undefined;
  const queries = defineQueries({
    byTime: defineQuery(argsCodec, ({args}: {args: DecodedArgs}) => {
      received = args;
      return builder.event.where('at', '=', args.at.getTime());
    }),
  });

  const result = await handleQueryRequest({
    // The server wires the registry to the handler; `.fn` decodes the wire args.
    handler: (name, args) => mustGetQuery(queries, name).fn({args, ctx: {}}),
    schema,
    query: {},
    // `args` over the wire is the encoded (JSON) form.
    body: ['transform', [{id: 'q1', name: 'byTime', args: [{at: 1000}]}]],
    userID: null,
  });

  // The handler saw the decoded Date...
  assert(received);
  expect(received.at).toBeInstanceOf(Date);
  expect(received.at.getTime()).toBe(1000);

  // ...and the produced AST filtered on the (re-encoded) value.
  assert(!Array.isArray(result));
  assert('queries' in result);
  const [q] = result.queries;
  assert('ast' in q);
  expect(q.ast).toMatchObject({
    table: 'event',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'at'},
      op: '=',
      right: {type: 'literal', value: 1000},
    },
  });
});

test('server mutator dispatch decodes codec args from the wire', async () => {
  let received: (DecodedArgs & {id: string}) | undefined;
  const serverMutators = defineMutators({
    event: {
      create: defineMutator(
        {
          decode: ({id, at}: {id: string; at: number}) => ({
            id,
            at: new Date(at),
          }),
          encode: ({id, at}: {id: string; at: Date}) => ({
            id,
            at: at.getTime(),
          }),
        },
        async ({
          args,
        }: {
          args: {id: string; at: Date};
          ctx: unknown;
          tx: unknown;
        }) => {
          received = args;
        },
      ),
    },
  });

  // This mirrors PushProcessor#dispatchMutation: look up by dotted name, then
  // invoke `.fn` with the encoded wire args.
  const mutator = getMutator(serverMutators, 'event.create');
  assert(mutator);
  await mutator.fn({
    args: {id: 'a', at: 2000},
    ctx: undefined,
    tx: {} as Transaction<Schema, unknown>,
  });

  assert(received);
  expect(received.at).toBeInstanceOf(Date);
  expect(received.at.getTime()).toBe(2000);
  expect(received.id).toBe('a');
});
