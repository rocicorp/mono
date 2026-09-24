import type {StandardSchemaV1} from '@standard-schema/spec';
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
import {
  addContextToQuery,
  defineQueries,
  defineQueryWithType,
  getQuery,
  mustGetQuery,
} from './query-registry.ts';
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

test('a validator that also has decode/encode methods is a validator, not a codec', () => {
  // Standard Schema implementations such as Zod 4 expose `.decode()` /
  // `.encode()` on every schema instance; they must keep taking the validator
  // path.
  const zodLike: StandardSchemaV1<EncodedArgs, EncodedArgs> & {
    decode: () => never;
    encode: () => never;
  } = {
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: value => ({value: value as EncodedArgs}),
    },
    'decode': () => {
      throw new Error('decode must not be called');
    },
    'encode': () => {
      throw new Error('encode must not be called');
    },
  };
  const def = defineQuery(zodLike, ({args}) =>
    builder.event.where('at', '=', args.at),
  );
  expect(def.validator).toBe(zodLike);
  expect(def.codec).toBeUndefined();

  const queries = defineQueries({byTime: def});
  const query = addContextToQuery(queries.byTime({at: 5}), {});
  expect(asQueryInternals(query).ast).toMatchObject({
    where: {right: {type: 'literal', value: 5}},
  });
});

test('codec query: undefined args bypass the codec', () => {
  const calls: string[] = [];
  const optionalCodec: Codec<EncodedArgs | undefined, DecodedArgs | undefined> =
    {
      decode: a => {
        calls.push('decode');
        return a && {at: new Date(a.at)};
      },
      encode: a => {
        calls.push('encode');
        return a && {at: a.at.getTime()};
      },
    };
  let received: DecodedArgs | undefined | 'unset' = 'unset';
  const queries = defineQueries({
    events: defineQuery(
      optionalCodec,
      ({args}: {args: DecodedArgs | undefined}) => {
        received = args;
        return args
          ? builder.event.where('at', '=', args.at.getTime())
          : builder.event;
      },
    ),
  });

  const qr = queries.events();
  expect(qr.args).toBeUndefined();
  addContextToQuery(qr, {});
  expect(received).toBeUndefined();
  expect(calls).toEqual([]);

  // Non-undefined args still go through the codec.
  expect(queries.events({at: new Date(3)}).args).toEqual({at: 3});
  expect(calls).toEqual(['encode']);
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

test('defineQueryWithType accepts a codec', () => {
  const defineAppQuery = defineQueryWithType<typeof schema, {userID: string}>();
  const queries = defineQueries({
    byTime: defineAppQuery(argsCodec, ({args, ctx}) => {
      expectTypeOf(args).toEqualTypeOf<DecodedArgs>();
      expectTypeOf(ctx).toEqualTypeOf<{userID: string}>();
      return builder.event.where('at', '=', args.at.getTime());
    }),
  });
  expectTypeOf(queries.byTime).parameter(0).toEqualTypeOf<DecodedArgs>();

  const query = addContextToQuery(queries.byTime({at: new Date(9)}), {
    userID: 'u',
  });
  expect(asQueryInternals(query).ast).toMatchObject({
    where: {right: {type: 'literal', value: 9}},
  });
});

test('defineQueryWithType: a validator with decode/encode resolves as a validator (type level)', () => {
  // Guards the signature order in TypedDefineQuery: a Zod-4-style schema
  // structurally satisfies Codec, so the validator signature must win and the
  // call site must be typed as the raw input, not the decoded output.
  const zodLike: StandardSchemaV1<EncodedArgs, DecodedArgs> & {
    decode: (v: EncodedArgs) => DecodedArgs;
    encode: (v: DecodedArgs) => EncodedArgs;
  } = {
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: value => ({value: {at: new Date((value as EncodedArgs).at)}}),
    },
    'decode': ({at}) => ({at: new Date(at)}),
    'encode': ({at}) => ({at: at.getTime()}),
  };
  const defineAppQuery = defineQueryWithType<typeof schema, {userID: string}>();
  const def = defineAppQuery(zodLike, ({args}) => {
    expectTypeOf(args).toEqualTypeOf<DecodedArgs>();
    return builder.event.where('at', '=', args.at.getTime());
  });
  expect(def.validator).toBe(zodLike);
  expect(def.codec).toBeUndefined();

  const queries = defineQueries({byTime: def});
  // Call site takes the (validated) input type, not the decoded type.
  expectTypeOf(queries.byTime).parameter(0).toEqualTypeOf<EncodedArgs>();
});

test('a codec query looked up by literal name takes decoded args and encodes once', () => {
  const queries = defineQueries({
    event: {
      byTime: defineQuery(argsCodec, ({args}: {args: DecodedArgs}) =>
        builder.event.where('at', '=', args.at.getTime()),
      ),
      byId: defineQuery(({args}: {args: string}) =>
        builder.event.where('id', '=', args),
      ),
    },
  });

  const byTime = mustGetQuery(queries, 'event.byTime');
  expectTypeOf(byTime).parameter(0).toEqualTypeOf<DecodedArgs>();
  expect(byTime({at: new Date(1000)}).args).toEqual({at: 1000});
  // Legacy '|' separators resolve the same way.
  expectTypeOf(mustGetQuery(queries, 'event|byTime'))
    .parameter(0)
    .toEqualTypeOf<DecodedArgs>();

  // A plain query in the same registry keeps its own args type.
  const byId = getQuery(queries, 'event.byId');
  assert(byId);
  expectTypeOf(byId).parameter(0).toEqualTypeOf<string>();
  expect(byId('x').args).toBe('x');

  // Type-only (never executed): the encoded form would be encoded again.
  const _rejectsEncoded = () => {
    // @ts-expect-error - a codec query takes the decoded args
    byTime({at: 1});
  };
  void _rejectsEncoded;
});
