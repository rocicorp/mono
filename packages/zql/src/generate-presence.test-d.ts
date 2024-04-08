import {assertType, expectTypeOf, test} from 'vitest';
import {z} from 'zod';
import {generatePresence} from './generate-presence.js';
import {ReadTransaction, WriteTransaction} from './generate.js';

const entryNoID = z
  .object({
    clientID: z.string(),
    str: z.string(),
    optStr: z.string().optional(),
  })
  .strict();

type EntryNoID = z.infer<typeof entryNoID>;

const {
  init: initEntryNoID,
  set: setEntryNoID,
  update: updateEntryNoID,
  delete: deleteEntryNoID,
  get: getEntryNoID,
  mustGet: mustGetEntryNoID,
  has: hasEntryNoID,
  list: listEntryNoID,
  listIDs: listIDsEntryNoID,
  listClientIDs: listClientIDsEntryNoID,
  listEntries: listEntriesEntryNoID,
} = generatePresence<EntryNoID>('entryNoID', entryNoID.parse);

const entryID = z
  .object({
    clientID: z.string(),
    id: z.string(),
    str: z.string(),
    optStr: z.string().optional(),
  })
  .strict();

type EntryID = z.infer<typeof entryID>;

const {
  init: initEntryID,
  set: setEntryID,
  update: updateEntryID,
  delete: deleteEntryID,
  get: getEntryID,
  mustGet: mustGetEntryID,
  has: hasEntryID,
  list: listEntryID,
  listIDs: listIDsEntryID,
  listClientIDs: listClientIDsEntryID,
  listEntries: listEntriesEntryID,
} = generatePresence<EntryID>('entryID', entryID.parse);

declare const rtx: ReadTransaction;
declare const wtx: WriteTransaction;

test('init', async () => {
  await initEntryID(wtx, {clientID: 'foo', id: 'bar', str: 'baz'});
  await initEntryID(wtx, {id: 'bar', str: 'baz'});
  // @ts-expect-error missing str
  await initEntryID(wtx, {id: 'bar'});
  // @ts-expect-error missing id
  await initEntryID(wtx, {clientID: 'foo', str: 'baz'});
  expectTypeOf(initEntryID).returns.resolves.toBeBoolean();

  await initEntryNoID(wtx, {clientID: 'foo', str: 'baz'});
  await initEntryNoID(wtx, {str: 'baz'});
  // @ts-expect-error Type 'number' is not assignable to type 'string'.
  await initEntryNoID(wtx, {clientID: 'foo', str: 42});
  // @ts-expect-error missing str
  await initEntryNoID(wtx, {});
  expectTypeOf(initEntryNoID).returns.resolves.toBeBoolean();
});

test('update', async () => {
  await updateEntryID(wtx, {clientID: 'foo', id: 'bar', str: 'baz'});
  await updateEntryID(wtx, {id: 'bar', str: 'baz'});
  await updateEntryID(wtx, {id: 'bar'});
  // @ts-expect-error missing id
  await updateEntryID(wtx, {clientID: 'foo', str: 'baz'});
  expectTypeOf(updateEntryID).returns.resolves.toBeVoid();

  await updateEntryNoID(wtx, {clientID: 'foo', str: 'baz'});
  await updateEntryNoID(wtx, {str: 'baz'});
  // @ts-expect-error Type 'number' is not assignable to type 'string'.
  await updateEntryNoID(wtx, {clientID: 'foo', str: 42});

  expectTypeOf(updateEntryNoID).returns.resolves.toBeVoid();
});

test('delete', async () => {
  await deleteEntryID(wtx, {clientID: 'cid', id: 'bar'});
  await deleteEntryID(wtx, {id: 'bar'});
  expectTypeOf(deleteEntryID)
    .parameter(1)
    .toEqualTypeOf<{clientID?: string | undefined; id: string} | undefined>();
  expectTypeOf(deleteEntryID).returns.resolves.toBeVoid();

  // @ts-expect-error extra str
  await deleteEntryID(wtx, {clientID: 'foo', id: 'bar', str: 'baz'});
  // @ts-expect-error extra str
  await deleteEntryID(wtx, {id: 'bar', str: 'baz'});
  // @ts-expect-error missing id
  await deleteEntryID(wtx, {clientID: 'foo'});
  // @ts-expect-error missing id
  await deleteEntryID(wtx, {});
  expectTypeOf(deleteEntryID)
    .parameter(1)
    .not.toEqualTypeOf<{clientID?: string | undefined} | undefined>();

  await deleteEntryNoID(wtx, {clientID: 'foo'});
  await deleteEntryNoID(wtx, {});
  await deleteEntryNoID(wtx, undefined);
  await deleteEntryNoID(wtx);
  expectTypeOf(deleteEntryNoID)
    .parameter(1)
    .toMatchTypeOf<{clientID?: string | undefined} | undefined>();
  expectTypeOf(deleteEntryNoID).returns.resolves.toBeVoid();

  // @ts-expect-error extra str
  await deleteEntryNoID(wtx, {clientID: 'foo', str: 'baz'});
  // @ts-expect-error extra str
  await deleteEntryNoID(wtx, {str: 'baz'});
  // @ts-expect-error Type 'number' is not assignable to type 'string'.
  await deleteEntryNoID(wtx, {clientID: 'foo', str: 42});
  expectTypeOf(deleteEntryNoID)
    .parameter(1)
    .not.toEqualTypeOf<
      {clientID?: string | undefined; id: string} | undefined
    >();
});

test('set', () => {
  assertType<
    (
      tx: WriteTransaction,
      id: {clientID?: string; id: string; str: string; optStr?: string},
    ) => Promise<void>
  >(setEntryID);

  assertType<
    (
      tx: WriteTransaction,
      id: {clientID?: string; str: string; optStr?: string},
    ) => Promise<void>
  >(setEntryNoID);
});

test('get', () => {
  assertType<
    (
      tx: ReadTransaction,
      id: {clientID?: string} | undefined,
    ) => Promise<EntryNoID | undefined>
  >(getEntryNoID);

  assertType<
    (
      tx: ReadTransaction,
      id: {clientID?: string; id: string},
    ) => Promise<EntryID | undefined>
  >(getEntryID);
  expectTypeOf(getEntryID).not.toMatchTypeOf<
    (
      tx: ReadTransaction,
      id: {clientID?: string},
    ) => Promise<EntryID | undefined>
  >();

  assertType<
    (
      tx: ReadTransaction,
      id: {clientID?: string},
    ) => Promise<EntryID | undefined>
    // @ts-expect-error missing id
  >(getEntryID);

  expectTypeOf(getEntryNoID)
    .parameter(1)
    .toEqualTypeOf<{clientID?: string} | undefined>();
  expectTypeOf(getEntryNoID).returns.resolves.toEqualTypeOf<
    EntryNoID | undefined
  >();

  expectTypeOf(getEntryID)
    .parameter(1)
    .toEqualTypeOf<{clientID?: string; id: string} | undefined>();
  expectTypeOf(getEntryID).returns.resolves.toEqualTypeOf<
    EntryID | undefined
  >();
});

test('mustGet', () => {
  expectTypeOf(mustGetEntryNoID)
    .parameter(1)
    .toEqualTypeOf<{clientID?: string} | undefined>();
  expectTypeOf(mustGetEntryNoID).returns.resolves.not.toBeUndefined();
  expectTypeOf(mustGetEntryNoID).returns.resolves.toEqualTypeOf<EntryNoID>();

  expectTypeOf(mustGetEntryID)
    .parameter(1)
    .toEqualTypeOf<{clientID?: string; id: string} | undefined>();
  expectTypeOf(mustGetEntryID).returns.resolves.not.toBeUndefined();
  expectTypeOf(mustGetEntryID).returns.resolves.toEqualTypeOf<EntryID>();
});

test('has', async () => {
  await hasEntryNoID(rtx, {clientID: 'foo'});
  await hasEntryNoID(rtx, {});
  await hasEntryNoID(rtx, undefined);
  await hasEntryNoID(rtx);
  assertType<
    (
      tx: ReadTransaction,
      id?: {clientID?: string} | undefined,
    ) => Promise<boolean>
  >(hasEntryNoID);

  // @ts-expect-error extra id
  await hasEntryNoID(rtx, {id: 'bar'});
  // @ts-expect-error extra str
  await hasEntryNoID(rtx, {str: 'bar'});

  await hasEntryID(rtx, {clientID: 'foo', id: 'b'});
  await hasEntryID(rtx, {id: 'b'});

  assertType<
    (
      tx: ReadTransaction,
      id: {clientID?: string; id: string},
    ) => Promise<boolean>
  >(hasEntryID);

  // @ts-expect-error missing id
  await hasEntryID(rtx, {});
  // @ts-expect-error missing id
  await hasEntryID(rtx, {clientID: 'foo'});
  // @ts-expect-error extra str
  await hasEntryID(rtx, {str: 'bar'});

  // TODO(arv): Fix these
  // await hasEntryID(rtx, undefined);
  // await hasEntryID(rtx);
});

test('list', async () => {
  assertType<EntryNoID[]>(await listEntryNoID(rtx));
  await listEntryNoID(rtx, {});
  await listEntryNoID(rtx, {limit: 1});
  await listEntryNoID(rtx, {startAtID: {clientID: 'cid'}});
  await listEntryNoID(rtx, {startAtID: {clientID: 'cid'}, limit: 0});
  // @ts-expect-error unknown property foo
  await listEntryNoID(rtx, {startAtID: {clientID: 'cid', foo: 'bar'}});
  // @ts-expect-error unknown property id
  await listEntryNoID(rtx, {startAtID: {clientID: 'cid', id: 'bar'}});

  assertType<EntryID[]>(await listEntryID(rtx));
  await listEntryID(rtx, {});
  await listEntryID(rtx, {limit: 1});
  await listEntryID(rtx, {startAtID: {clientID: 'cid'}});
  await listEntryID(rtx, {startAtID: {clientID: 'cid'}, limit: 0});
  await listEntryID(rtx, {startAtID: {clientID: 'cid', id: 'b'}});
  // @ts-expect-error unknown property foo
  await listEntryID(rtx, {startAtID: {clientID: 'cid', id: 'b', foo: 'bar'}});
});

test('listIDs', async () => {
  assertType<{clientID: string}[]>(await listIDsEntryNoID(rtx));
  await listIDsEntryNoID(rtx, {});
  await listIDsEntryNoID(rtx, {limit: 1});
  await listIDsEntryNoID(rtx, {startAtID: {clientID: 'cid'}});
  await listIDsEntryNoID(rtx, {startAtID: {clientID: 'cid'}, limit: 0});
  // @ts-expect-error unknown property foo
  await listIDsEntryNoID(rtx, {startAtID: {clientID: 'cid', foo: 'bar'}});
  // @ts-expect-error unknown property id
  await listIDsEntryNoID(rtx, {startAtID: {clientID: 'cid', id: 'bar'}});

  assertType<{clientID: string; id: string}[]>(await listIDsEntryID(rtx));
  await listIDsEntryID(rtx, {});
  await listIDsEntryID(rtx, {limit: 1});
  await listIDsEntryID(rtx, {startAtID: {clientID: 'cid'}});
  await listIDsEntryID(rtx, {startAtID: {clientID: 'cid'}, limit: 0});
  await listIDsEntryID(rtx, {startAtID: {clientID: 'cid', id: 'b'}});
  await listIDsEntryID(rtx, {
    // @ts-expect-error unknown property foo
    startAtID: {clientID: 'cid', id: 'b', foo: 'bar'},
  });
});

test('listClientIDs', async () => {
  assertType<string[]>(await listClientIDsEntryNoID(rtx));
  await listClientIDsEntryNoID(rtx, {});
  await listClientIDsEntryNoID(rtx, {limit: 1});
  await listClientIDsEntryNoID(rtx, {startAtID: {clientID: 'cid'}});
  await listClientIDsEntryNoID(rtx, {startAtID: {clientID: 'cid'}, limit: 0});
  // @ts-expect-error unknown property foo
  await listClientIDsEntryNoID(rtx, {startAtID: {clientID: 'cid', foo: 'bar'}});
  // @ts-expect-error unknown property id
  await listClientIDsEntryNoID(rtx, {startAtID: {clientID: 'cid', id: 'bar'}});

  assertType<string[]>(await listClientIDsEntryID(rtx));
  await listClientIDsEntryID(rtx, {});
  await listClientIDsEntryID(rtx, {limit: 1});
  await listClientIDsEntryID(rtx, {startAtID: {clientID: 'cid'}});
  await listClientIDsEntryID(rtx, {startAtID: {clientID: 'cid'}, limit: 0});
  await listClientIDsEntryID(rtx, {startAtID: {clientID: 'cid', id: 'b'}});
  await listClientIDsEntryID(rtx, {
    // @ts-expect-error unknown property foo
    startAtID: {clientID: 'cid', id: 'b', foo: 'bar'},
  });
});

test('listEntries', async () => {
  assertType<[{clientID: string}, EntryNoID][]>(
    await listEntriesEntryNoID(rtx),
  );
  await listEntriesEntryNoID(rtx, {});
  await listEntriesEntryNoID(rtx, {limit: 1});
  await listEntriesEntryNoID(rtx, {startAtID: {clientID: 'cid'}});
  await listEntriesEntryNoID(rtx, {startAtID: {clientID: 'cid'}, limit: 0});
  // @ts-expect-error unknown property foo
  await listEntriesEntryNoID(rtx, {startAtID: {clientID: 'cid', foo: 'bar'}});
  // @ts-expect-error unknown property id
  await listEntriesEntryNoID(rtx, {startAtID: {clientID: 'cid', id: 'bar'}});

  assertType<[{clientID: string; id: string}, EntryID][]>(
    await listEntriesEntryID(rtx),
  );
  await listEntriesEntryID(rtx, {});
  await listEntriesEntryID(rtx, {limit: 1});
  await listEntriesEntryID(rtx, {startAtID: {clientID: 'cid'}});
  await listEntriesEntryID(rtx, {startAtID: {clientID: 'cid'}, limit: 0});
  await listEntriesEntryID(rtx, {startAtID: {clientID: 'cid', id: 'b'}});
  await listEntriesEntryID(rtx, {
    // @ts-expect-error unknown property foo
    startAtID: {clientID: 'cid', id: 'b', foo: 'bar'},
  });
});
