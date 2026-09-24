// oxlint-disable require-await
import type {StandardSchemaV1} from '@standard-schema/spec';
import {assert, expect, expectTypeOf, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Codec} from '../../../zero-types/src/schema-value.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Transaction} from './custom.ts';
import {
  defineMutators,
  defineMutatorsWithType,
  getMutator,
  mustGetMutator,
} from './mutator-registry.ts';
import {defineMutator} from './mutator.ts';

const schema = createSchema({
  tables: [
    table('event').columns({id: string(), at: number()}).primaryKey('id'),
  ],
});

// Decoded args expose `at` as a Date; encoded (wire) args use epoch millis.
type DecodedArgs = {id: string; at: Date};
type EncodedArgs = {id: string; at: number};

const argsCodec: Codec<EncodedArgs, DecodedArgs> = {
  decode: ({id, at}) => ({id, at: new Date(at)}),
  encode: ({id, at}) => ({id, at: at.getTime()}),
};

test('defineMutator stores the codec on the definition', () => {
  const def = defineMutator(argsCodec, async () => {});
  expect(def.codec).toBe(argsCodec);
  expect(def.validator).toBeUndefined();
});

test('codec mutator: callable encodes args to the wire form', () => {
  const mutators = defineMutators({
    event: {
      create: defineMutator(argsCodec, async () => {}),
    },
  });

  const mr = mutators.event.create({id: 'a', at: new Date(1000)});

  // The stored / wire args are the encoded JSON form.
  expect(mr.args).toEqual({id: 'a', at: 1000});
});

test('codec mutator: fn decodes the wire args before the recipe runs', async () => {
  let received: DecodedArgs | undefined;
  const mutators = defineMutatorsWithType<typeof schema>()({
    event: {
      create: defineMutator(
        argsCodec,
        async ({args}: {args: DecodedArgs; ctx: unknown; tx: unknown}) => {
          received = args;
        },
      ),
    },
  });

  // The framework invokes `fn` with the encoded (wire/stored) args.
  await mutators.event.create.fn({
    args: {id: 'a', at: 2000},
    ctx: undefined,
    tx: {} as Transaction<typeof schema, unknown>,
  });

  assert(received);
  expect(received.at).toBeInstanceOf(Date);
  expect(received.at.getTime()).toBe(2000);
  expect(received.id).toBe('a');
});

test('codec mutator: round-trips a Date through encode + decode', async () => {
  let received: DecodedArgs | undefined;
  const mutators = defineMutatorsWithType<typeof schema>()({
    event: {
      create: defineMutator(
        argsCodec,
        async ({args}: {args: DecodedArgs; ctx: unknown; tx: unknown}) => {
          received = args;
        },
      ),
    },
  });

  const mr = mutators.event.create({id: 'a', at: new Date(1234)});
  await mutators.event.create.fn({
    args: mr.args,
    ctx: undefined,
    tx: {} as Transaction<typeof schema, unknown>,
  });

  assert(received);
  expect(received.at.getTime()).toBe(1234);
});

test('codec mutator: the callable accepts the decoded type', () => {
  const mutators = defineMutatorsWithType<typeof schema>()({
    event: {
      create: defineMutator(
        argsCodec,
        async ({args}: {args: DecodedArgs; ctx: unknown; tx: unknown}) => {
          void args;
        },
      ),
    },
  });

  const mr = mutators.event.create({id: 'a', at: new Date(1)});
  // Callable is typed to accept the decoded args (Date), not the encoded form.
  expectTypeOf(mutators.event.create).parameter(0).toEqualTypeOf<DecodedArgs>();
  // The stored args type stays the encoded (JSON) form.
  expectTypeOf(mr['~']['$input']).toEqualTypeOf<EncodedArgs>();

  // Type-only (never executed): the encoded form is rejected at the call site.
  const _rejectsEncoded = () => {
    // @ts-expect-error - cannot pass the encoded form to a codec mutator
    mutators.event.create({id: 'a', at: 1});
  };
  void _rejectsEncoded;
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
  const def = defineMutator(zodLike, async () => {});
  expect(def.validator).toBe(zodLike);
  expect(def.codec).toBeUndefined();

  const mutators = defineMutators({event: {create: def}});
  expect(mutators.event.create({id: 'a', at: 1}).args).toEqual({
    id: 'a',
    at: 1,
  });
});

test('codec mutator: undefined args bypass the codec', async () => {
  const calls: string[] = [];
  const optionalCodec: Codec<EncodedArgs | undefined, DecodedArgs | undefined> =
    {
      decode: a => {
        calls.push('decode');
        return a && {id: a.id, at: new Date(a.at)};
      },
      encode: a => {
        calls.push('encode');
        return a && {id: a.id, at: a.at.getTime()};
      },
    };
  let received: DecodedArgs | undefined | 'unset' = 'unset';
  const mutators = defineMutators({
    event: {
      touch: defineMutator(
        optionalCodec,
        async ({
          args,
        }: {
          args: DecodedArgs | undefined;
          ctx: unknown;
          tx: unknown;
        }) => {
          received = args;
        },
      ),
    },
  });

  const mr = mutators.event.touch();
  expect(mr.args).toBeUndefined();
  await mutators.event.touch.fn({
    args: undefined,
    ctx: undefined,
    tx: {} as Transaction<Schema, unknown>,
  });
  expect(received).toBeUndefined();
  expect(calls).toEqual([]);

  // Non-undefined args still go through the codec.
  expect(mutators.event.touch({id: 'a', at: new Date(3)}).args).toEqual({
    id: 'a',
    at: 3,
  });
  expect(calls).toEqual(['encode']);
});

test('non-codec mutators are unaffected', () => {
  const mutators = defineMutators({
    event: {
      plain: defineMutator(
        async ({args}: {args: {id: string}; ctx: unknown; tx: unknown}) => {
          void args;
        },
      ),
    },
  });
  const mr = mutators.event.plain({id: 'a'});
  expect(mr.args).toEqual({id: 'a'});
});

test('a validator with a non-JSON output resolves as a validator, not a codec (type level)', () => {
  // Zod 4 schemas expose decode/encode and may transform to a non-JSON type
  // (e.g. a Date). They must take the validator signature, so the callable
  // accepts the raw (validated) input rather than the decoded output.
  const zodLike: StandardSchemaV1<EncodedArgs, DecodedArgs> & {
    decode: (v: EncodedArgs) => DecodedArgs;
    encode: (v: DecodedArgs) => EncodedArgs;
  } = {
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: value => {
        const {id, at} = value as EncodedArgs;
        return {value: {id, at: new Date(at)}};
      },
    },
    'decode': ({id, at}) => ({id, at: new Date(at)}),
    'encode': ({id, at}) => ({id, at: at.getTime()}),
  };
  const def = defineMutator(zodLike, async ({args}) => {
    expectTypeOf(args).toEqualTypeOf<DecodedArgs>();
  });
  expect(def.validator).toBe(zodLike);
  expect(def.codec).toBeUndefined();

  const mutators = defineMutators({event: {create: def}});
  expectTypeOf(mutators.event.create).parameter(0).toEqualTypeOf<EncodedArgs>();
  // The raw input is stored as-is (no encode).
  expect(mutators.event.create({id: 'a', at: 1}).args).toEqual({
    id: 'a',
    at: 1,
  });
});

test('a codec mutator looked up by literal name takes decoded args and encodes once', () => {
  const mutators = defineMutators({
    event: {
      create: defineMutator(argsCodec, async () => {}),
      plain: defineMutator(
        async ({args}: {args: {id: string}; ctx: unknown; tx: unknown}) => {
          void args;
        },
      ),
    },
  });

  const create = mustGetMutator(mutators, 'event.create');
  expectTypeOf(create).parameter(0).toEqualTypeOf<DecodedArgs>();
  expect(create({id: 'a', at: new Date(1000)}).args).toEqual({
    id: 'a',
    at: 1000,
  });

  // A plain mutator in the same registry keeps its own args type.
  const plain = getMutator(mutators, 'event.plain');
  assert(plain);
  expectTypeOf(plain).parameter(0).toEqualTypeOf<{id: string}>();
  expect(plain({id: 'b'}).args).toEqual({id: 'b'});

  // Type-only (never executed): the encoded form would be encoded again.
  const _rejectsEncoded = () => {
    // @ts-expect-error - a codec mutator takes the decoded args
    create({id: 'a', at: 1});
  };
  void _rejectsEncoded;
});
