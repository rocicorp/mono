// oxlint-disable require-await
import {assert, expect, expectTypeOf, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Codec} from '../../../zero-types/src/schema-value.ts';
import type {Transaction} from './custom.ts';
import {defineMutators, defineMutatorsWithType} from './mutator-registry.ts';
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
