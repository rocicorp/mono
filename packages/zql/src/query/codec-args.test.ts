import {assert, expect, expectTypeOf, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Codec} from '../../../zero-types/src/schema-value.ts';
import {createBuilder} from './create-builder.ts';
import {asQueryInternals} from './query-internals.ts';
import {addContextToQuery, defineQueries} from './query-registry.ts';
import {defineQuery} from './query-registry.ts';

const schema = createSchema({
  tables: [
    table('event').columns({id: string(), at: number()}).primaryKey('id'),
  ],
});
const builder = createBuilder(schema);

// Decoded args expose `at` as a Date; encoded (wire) args use epoch millis.
type DecodedArgs = {at: Date};
type EncodedArgs = {at: number};

const argsCodec: Codec<EncodedArgs, DecodedArgs> = {
  decode: ({at}) => ({at: new Date(at)}),
  encode: ({at}) => ({at: at.getTime()}),
};

test('defineQuery stores the codec on the definition', () => {
  const def = defineQuery(argsCodec, ({args}) =>
    builder.event.where('at', '=', args.at.getTime()),
  );
  expect(def.codec).toBe(argsCodec);
  expect(def.validator).toBeUndefined();
});

test('codec query: callable encodes args to the wire form', () => {
  const queries = defineQueries({
    byTime: defineQuery(argsCodec, ({args}: {args: DecodedArgs}) =>
      builder.event.where('at', '=', args.at.getTime()),
    ),
  });

  const qr = queries.byTime({at: new Date(1000)});
  expect(qr.args).toEqual({at: 1000});
});

test('codec query: fn decodes the wire args before the query fn runs', () => {
  let received: DecodedArgs | undefined;
  const queries = defineQueries({
    byTime: defineQuery(argsCodec, ({args}: {args: DecodedArgs}) => {
      received = args;
      return builder.event.where('at', '=', args.at.getTime());
    }),
  });

  // The callable encodes; addContextToQuery runs the fn, which decodes.
  const query = addContextToQuery(queries.byTime({at: new Date(2000)}), {});

  assert(received);
  expect(received.at).toBeInstanceOf(Date);
  expect(received.at.getTime()).toBe(2000);
  // The decoded Date drove the query literal (encoded back to epoch millis).
  expect(asQueryInternals(query).ast).toMatchObject({
    table: 'event',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'at'},
      op: '=',
      right: {type: 'literal', value: 2000},
    },
  });
});

test('codec query: the callable accepts the decoded type', () => {
  const queries = defineQueries({
    byTime: defineQuery(argsCodec, ({args}: {args: DecodedArgs}) =>
      builder.event.where('at', '=', args.at.getTime()),
    ),
  });

  const qr = queries.byTime({at: new Date(1)});
  expectTypeOf(queries.byTime).parameter(0).toEqualTypeOf<DecodedArgs>();
  expectTypeOf(qr['~']['$input']).toEqualTypeOf<EncodedArgs>();

  // Type-only (never executed): the encoded form is rejected at the call site.
  const _rejectsEncoded = () => {
    // @ts-expect-error - cannot pass the encoded form to a codec query
    queries.byTime({at: 1});
  };
  void _rejectsEncoded;
});
