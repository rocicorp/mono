/* eslint-disable @typescript-eslint/naming-convention */
import {OptionalLogger} from '@rocicorp/logger';
import {Reflect} from '@rocicorp/reflect/client';
import {nanoid} from 'nanoid';
import {MutatorDefs, Replicache, TEST_LICENSE_KEY} from 'replicache';
import {expect, suite, test} from 'vitest';
import {ZodError, z} from 'zod';
import {
  PresenceEntity,
  PresenceListOptions,
  generatePresence,
  keyFromID,
  normalizeScanOptions,
  parseKeyToID,
} from './generate-presence.js';
import {WriteTransaction} from './generate.js';
import {ReadonlyJSONValue} from './json.js';

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

const collectionNames = ['entryNoID', 'entryID'] as const;

function sameForBoth<C extends object>(
  c: C,
): [C & {collectionName: 'entryNoID'}, C & {collectionName: 'entryID'}] {
  return collectionNames.map(collectionName => ({...c, collectionName})) as [
    C & {collectionName: 'entryNoID'},
    C & {collectionName: 'entryID'},
  ];
}

async function directWrite(
  tx: WriteTransaction,
  {key, val}: {key: string; val: ReadonlyJSONValue},
) {
  await tx.set(key, val);
}

const mutators = {
  initEntryNoID,
  setEntryNoID,
  updateEntryNoID,
  deleteEntryNoID,
  listEntryNoID,

  initEntryID,
  setEntryID,
  updateEntryID,
  deleteEntryID,
  listEntryID,

  directWrite,
};

const factories = [
  <M extends MutatorDefs>(m: M) =>
    new Replicache({
      licenseKey: TEST_LICENSE_KEY,
      name: nanoid(),
      mutators: m,
    }),
  <M extends MutatorDefs>(m: M) =>
    new Reflect({
      roomID: nanoid(),
      userID: nanoid(),
      mutators: m,
    }),
] as const;

suite('set', () => {
  type Case = {
    name: string;
    value: ReadonlyJSONValue | undefined;
    expectedKey?: string;
    expectedValue?: ReadonlyJSONValue;
    expectError?: ReadonlyJSONValue;
    collectionName: 'entryNoID' | 'entryID';
  };

  const clientID = '$CLIENT_ID';
  const id = 'b';

  const cases: Case[] = [
    {
      name: 'set with clientID (no id)',
      collectionName: 'entryNoID',
      value: {clientID, str: 'foo'},
      expectedValue: {clientID, str: 'foo'},
      expectedKey: `-/p/${clientID}/entryNoID/`,
    },
    {
      name: 'set with clientID (no id)',
      collectionName: 'entryID',
      value: {clientID, str: 'foo'},
      expectError: {_errors: [], id: {_errors: ['Required']}},
    },

    {
      name: 'set with clientID and id',
      collectionName: 'entryNoID',
      value: {clientID, id: 'a', str: 'foo'},
      expectError: {
        _errors: ["Unrecognized key(s) in object: 'id'"],
      },
    },
    {
      name: 'set with clientID and id',
      collectionName: 'entryID',
      value: {clientID, id: 'a', str: 'foo'},
      expectedValue: {clientID, id: 'a', str: 'foo'},
      expectedKey: '-/p/$CLIENT_ID/entryID/id/a',
    },
    {
      name: 'set with implicit clientID (no id)',
      collectionName: 'entryNoID',
      value: {str: 'foo'},
      expectedValue: {clientID, str: 'foo'},
      expectedKey: `-/p/${clientID}/entryNoID/`,
    },
    {
      name: 'set with implicit clientID (no id)',
      collectionName: 'entryID',
      value: {str: 'foo'},
      expectError: {_errors: [], id: {_errors: ['Required']}},
    },

    ...sameForBoth({
      name: 'set to null',
      value: null,
      expectError: 'TypeError: Expected object, received null',
    }),

    ...sameForBoth({
      name: 'set to undefined',
      value: undefined,
      expectError: 'TypeError: Expected object, received undefined',
    }),

    ...sameForBoth({
      name: 'set to string',
      value: 'junk',
      expectError: 'TypeError: Expected object, received string',
    }),

    {
      name: 'set to value missing str',
      collectionName: 'entryNoID',
      value: {},
      expectError: {_errors: [], str: {_errors: ['Required']}},
    },
    {
      name: 'set to value missing str',
      collectionName: 'entryID',
      value: {id: 'c'},
      expectError: {_errors: [], str: {_errors: ['Required']}},
    },

    {
      name: 'set with optStr',
      collectionName: 'entryNoID',
      value: {str: 'foo', optStr: 'bar'},
      expectedValue: {clientID, str: 'foo', optStr: 'bar'},
      expectedKey: `-/p/${clientID}/entryNoID/`,
    },
    {
      name: 'set with optStr',
      collectionName: 'entryID',
      value: {str: 'foo', id, optStr: 'bar'},
      expectedValue: {clientID, id, str: 'foo', optStr: 'bar'},
      expectedKey: `-/p/${clientID}/entryID/id/${id}`,
    },

    {
      name: 'setting with wrong clientID',
      collectionName: 'entryNoID',
      value: {clientID: 'wrong', str: 'foo'},
      expectError: `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
    {
      name: 'setting with wrong clientID',
      collectionName: 'entryID',
      value: {clientID: 'wrong', id: 'v', str: 'foo'},
      expectError: `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(`${c.name} using ${c.collectionName}`, async () => {
        const {expectedValue, collectionName} = c;
        const r = f(mutators);
        const clientID = await r.clientID;
        const replace = <V extends ReadonlyJSONValue | undefined>(v: V): V =>
          replaceClientID(v, '$CLIENT_ID', clientID) as V;
        let error = undefined;
        try {
          if (collectionName === 'entryID') {
            await r.mutate.setEntryID(replace(c.value) as EntryID);
          } else {
            await r.mutate.setEntryNoID(replace(c.value) as EntryNoID);
          }
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = String(e);
          }
        }

        if (c.expectError) {
          expect(error).toEqual(replace(c.expectError));
        } else {
          const key = replaceClientID(c.expectedKey!, '$CLIENT_ID', clientID);
          const actual = await r.query(tx => tx.get(key));
          expect(actual).toEqual(replace(expectedValue));
        }
      });
    }
  }
});

suite('init', () => {
  type Case = {
    name: string;
    collectionName: 'entryNoID' | 'entryID';
    value: ReadonlyJSONValue | undefined;
    expectedKey: string;
    expectedValue: ReadonlyJSONValue | undefined;
    expectError?: ReadonlyJSONValue;
    preexisting?: ReadonlyJSONValue;
  };

  const clientID = '$CLIENT_ID';

  const cases: Case[] = [
    {
      name: 'null',
      collectionName: 'entryNoID',
      value: null,
      expectError: 'TypeError: Expected object, received null',
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedValue: undefined,
    },
    {
      name: 'null',
      collectionName: 'entryID',
      value: null,
      expectError: 'TypeError: Expected object, received null',
      expectedKey: `-/p/${clientID}/entryID/id/a`,
      expectedValue: undefined,
    },
    {
      name: 'undefined',
      collectionName: 'entryNoID',
      value: undefined,
      expectError: 'TypeError: Expected object, received undefined',
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedValue: undefined,
    },
    {
      name: 'undefined',
      collectionName: 'entryID',
      value: undefined,
      expectError: 'TypeError: Expected object, received undefined',
      expectedKey: `-/p/${clientID}/entryID/id/b`,
      expectedValue: undefined,
    },
    {
      name: 'string',
      collectionName: 'entryNoID',
      value: 'junk',
      expectError: 'TypeError: Expected object, received string',
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedValue: undefined,
    },
    {
      name: 'string',
      collectionName: 'entryID',
      value: 'junk',
      expectError: 'TypeError: Expected object, received string',
      expectedKey: `-/p/${clientID}/entryID/id/b`,
      expectedValue: undefined,
    },
    {
      name: 'init with clientID',
      collectionName: 'entryNoID',
      value: {clientID, str: 'foo'},
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedValue: {clientID, str: 'foo'},
    },
    {
      name: 'init with clientID and id',
      collectionName: 'entryID',
      value: {clientID, id: 'a', str: 'foo'},
      expectedKey: `-/p/${clientID}/entryID/id/a`,
      expectedValue: {clientID, id: 'a', str: 'foo'},
    },

    {
      name: 'init with clientID with preexisting',
      collectionName: 'entryNoID',
      preexisting: {clientID, str: 'before'},
      value: {clientID, str: 'foo'},
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedValue: {clientID, str: 'before'},
    },
    {
      name: 'init with clientID and id with preexisting',
      collectionName: 'entryID',
      preexisting: {clientID, id: 'a', str: 'before'},
      value: {clientID, id: 'a', str: 'foo'},
      expectedKey: `-/p/${clientID}/entryID/id/a`,
      expectedValue: {clientID, id: 'a', str: 'before'},
    },

    {
      name: 'no str',
      collectionName: 'entryNoID',
      value: {clientID},
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedValue: undefined,
      expectError: {_errors: [], str: {_errors: ['Required']}},
    },
    {
      name: 'no str',
      collectionName: 'entryID',
      value: {clientID, id: 'c'},
      expectedKey: `-/p/${clientID}/entryID/id/c`,
      expectedValue: undefined,
      expectError: {_errors: [], str: {_errors: ['Required']}},
    },

    {
      name: 'with optStr',
      collectionName: 'entryNoID',
      value: {clientID, str: 'x', optStr: 'y'},
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedValue: {clientID, str: 'x', optStr: 'y'},
    },
    {
      name: 'with optStr',
      collectionName: 'entryID',
      value: {clientID, id: 'z', str: 'x', optStr: 'y'},
      expectedKey: `-/p/${clientID}/entryID/id/z`,
      expectedValue: {clientID, id: 'z', str: 'x', optStr: 'y'},
    },

    {
      name: 'with wrong clientID',
      collectionName: 'entryNoID',
      value: {clientID: 'wrong', str: 'x'},
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedValue: undefined,
      expectError: `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
    {
      name: 'with wrong clientID',
      collectionName: 'entryID',
      value: {clientID: 'wrong', id: 'z', str: 'x'},
      expectedKey: `-/p/${clientID}/entryID/id/z`,
      expectedValue: undefined,
      expectError: `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {
          expectError,
          expectedKey,
          expectedValue,
          collectionName,
          preexisting,
          value,
        } = replaceClientID(c, '$CLIENT_ID', clientID);

        if (preexisting) {
          if (collectionName === 'entryID') {
            await r.mutate.setEntryID(preexisting as EntryID);
          } else {
            await r.mutate.setEntryNoID(preexisting as EntryNoID);
          }
        }

        let error;
        let result;
        try {
          if (collectionName === 'entryID') {
            result = await r.mutate.initEntryID(value as EntryID);
          } else {
            result = await r.mutate.initEntryNoID(value as EntryNoID);
          }
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = String(e);
          }
        }

        const key = expectedKey;
        const actual = await r.query(tx => tx.get(key));

        if (expectError) {
          expect(error).toEqual(expectError);
          expect(actual).toEqual(preexisting);
          expect(result).undefined;
        } else {
          expect(error).undefined;
          expect(actual).toEqual(expectedValue);
        }
      });
    }
  }
});

function replaceClientID<V extends ReadonlyJSONValue | undefined>(
  v: V,
  from: string,
  to: string,
): V {
  switch (typeof v) {
    case 'string':
      return v.replaceAll(from, to) as V;
    case 'object':
      if (v === null) {
        return v;
      }
      if (Array.isArray(v)) {
        return v.map(v => replaceClientID(v, from, to)) as unknown as V;
      }
      return Object.fromEntries(
        Object.entries(v).map(([k, v]) => [
          replaceClientID(k, from, to),
          replaceClientID(v, from, to),
        ]),
      ) as V;
    default:
      return v;
  }
}

suite('get', () => {
  type Case = {
    name: string;
    collectionName: 'entryNoID' | 'entryID';
    stored?: ReadonlyJSONValue | undefined;
    storedKey?: string;
    param: ReadonlyJSONValue | undefined;
    expectedValue?: ReadonlyJSONValue | undefined;
    expectedError?: ReadonlyJSONValue;
  };

  const clientID = '$CLIENT_ID';
  const id = 'b';

  const cases: Case[] = [
    {
      name: 'Get with clientID, existing',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {clientID},
    },
    {
      name: 'Get with clientID and id, existing',
      collectionName: 'entryID',
      stored: {clientID, id: 'a', str: 'foo'},
      param: {clientID, id: 'a'},
    },

    {
      name: 'Get with clientID, non existing',
      collectionName: 'entryNoID',
      stored: undefined,
      param: {clientID},
    },
    {
      name: 'Get with clientID and id, non existing',
      collectionName: 'entryID',
      stored: undefined,
      param: {clientID, id: 'a'},
    },

    {
      name: 'Get with implicit clientID, existing',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {},
    },
    {
      name: 'Get with implicit clientID and id, existing',
      collectionName: 'entryID',
      stored: {clientID, id: 'a', str: 'foo'},
      param: {id: 'a'},
    },
    {
      name: 'Get with no param, existing',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: undefined,
    },
    {
      name: 'Get with no param when id is required, existing',
      collectionName: 'entryNoID',
      stored: {clientID, id: 'a', str: 'foo'},
      param: undefined,
      expectedValue: undefined,
    },

    {
      name: 'Stored value is incorrect (extra id)',
      collectionName: 'entryNoID',
      stored: {clientID, id: 'a', str: 'foo'},
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {clientID},
      expectedError: {
        _errors: ["Unrecognized key(s) in object: 'id'"],
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (missing id)',
      collectionName: 'entryID',
      stored: {clientID, str: 'foo'},
      storedKey: '-/p/$CLIENT_ID/entryID/id/a',
      param: {clientID, id: 'a'},
      expectedError: {
        _errors: [],
        id: {_errors: ['Required']},
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (missing clientID)',
      collectionName: 'entryNoID',
      stored: {str: 'foo'},
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {clientID},
      expectedError: {
        _errors: [],
        clientID: {_errors: ['Required']},
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (missing id and clientID)',
      collectionName: 'entryID',
      stored: {str: 'foo'},
      storedKey: '-/p/$CLIENT_ID/entryID/id/a',
      param: {clientID, id: 'a'},
      expectedError: {
        _errors: [],
        clientID: {_errors: ['Required']},
        id: {_errors: ['Required']},
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (missing str)',
      collectionName: 'entryNoID',
      stored: {clientID},
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {clientID},
      expectedError: {
        _errors: [],
        str: {_errors: ['Required']},
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (wrong optStr)',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'a', optStr: true},
      storedKey: `-/p/${clientID}/entryID/id/${id}`,
      param: {clientID, id},
      expectedError: {
        _errors: [],
        optStr: {_errors: ['Expected string, received boolean']},
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (null)',
      collectionName: 'entryNoID',
      stored: null,
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {clientID},
      expectedError: {
        _errors: ['Expected object, received null'],
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (string)',
      collectionName: 'entryID',
      stored: 'junk',
      storedKey: '-/p/$CLIENT_ID/entryID/id/a',
      param: {clientID, id: 'a'},
      expectedError: {
        _errors: ['Expected object, received string'],
      },
      expectedValue: undefined,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(`${c.name} using ${c.collectionName}`, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;

        const fixedC = replaceClientID(c, '$CLIENT_ID', clientID);
        const {
          collectionName,
          stored,
          storedKey = stored
            ? keyFromID(collectionName, stored as PresenceEntity)
            : undefined,
          param,
          expectedError,
        } = fixedC;
        const expectedValue =
          'expectedValue' in fixedC ? fixedC.expectedValue : stored;

        if (stored !== undefined && storedKey !== undefined) {
          await r.mutate.directWrite({
            key: storedKey,
            val: stored,
          });
        }
        const {actual, error} = await r.query(async tx => {
          try {
            return {
              actual:
                collectionName === 'entryNoID'
                  ? await getEntryNoID(tx, param as EntryNoID)
                  : await getEntryID(tx, param as EntryID),
            };
          } catch (e) {
            return {error: (e as ZodError).format()};
          }
        });

        expect(error).toEqual(expectedError);
        expect(actual).toEqual(expectedValue);
      });
    }
  }
});

suite('mustGet', () => {
  type Case = {
    name: string;
    collectionName: 'entryNoID' | 'entryID';
    stored?: ReadonlyJSONValue | undefined;
    storedKey?: string;
    param: ReadonlyJSONValue | undefined;
    expectedValue?: ReadonlyJSONValue | undefined;
    expectedError?: ReadonlyJSONValue;
  };

  const clientID = '$CLIENT_ID';
  const id = 'b';

  const cases: Case[] = [
    {
      name: 'Get with clientID, existing',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {clientID},
    },
    {
      name: 'Get with clientID and id, existing',
      collectionName: 'entryID',
      stored: {clientID, id: 'a', str: 'foo'},
      param: {clientID, id: 'a'},
    },

    {
      name: 'Get with clientID, non existing',
      collectionName: 'entryNoID',
      stored: undefined,
      param: {clientID},
      expectedError: `Error: no such entity {"clientID":"${clientID}"}`,
      expectedValue: undefined,
    },
    {
      name: 'Get with clientID and id, non existing',
      collectionName: 'entryID',
      stored: undefined,
      param: {clientID, id: 'a'},
      expectedError: `Error: no such entity {"clientID":"${clientID}","id":"a"}`,
      expectedValue: undefined,
    },

    {
      name: 'Get with implicit clientID, existing',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {},
    },
    {
      name: 'Get with implicit clientID and id, existing',
      collectionName: 'entryID',
      stored: {clientID, id: 'a', str: 'foo'},
      param: {id: 'a'},
    },
    {
      name: 'Get with no param, existing',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: undefined,
    },
    {
      name: 'Get with no param when id is required, existing',
      collectionName: 'entryNoID',
      stored: {clientID, id: 'a', str: 'foo'},
      param: undefined,
      expectedError: `Error: no such entity {"clientID":"${clientID}"}`,
      expectedValue: undefined,
    },

    {
      name: 'Stored value is incorrect (extra id)',
      collectionName: 'entryNoID',
      stored: {clientID, id: 'a', str: 'foo'},
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {clientID},
      expectedError: {
        _errors: ["Unrecognized key(s) in object: 'id'"],
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (missing id)',
      collectionName: 'entryID',
      stored: {clientID, str: 'foo'},
      storedKey: '-/p/$CLIENT_ID/entryID/id/a',
      param: {clientID, id: 'a'},
      expectedError: {
        _errors: [],
        id: {_errors: ['Required']},
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (missing clientID)',
      collectionName: 'entryNoID',
      stored: {str: 'foo'},
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {clientID},
      expectedError: {
        _errors: [],
        clientID: {_errors: ['Required']},
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (missing id and clientID)',
      collectionName: 'entryID',
      stored: {str: 'foo'},
      storedKey: '-/p/$CLIENT_ID/entryID/id/a',
      param: {clientID, id: 'a'},
      expectedError: {
        _errors: [],
        clientID: {_errors: ['Required']},
        id: {_errors: ['Required']},
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (missing str)',
      collectionName: 'entryNoID',
      stored: {clientID},
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {clientID},
      expectedError: {
        _errors: [],
        str: {_errors: ['Required']},
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (wrong optStr)',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'a', optStr: true},
      storedKey: `-/p/${clientID}/entryID/id/${id}`,
      param: {clientID, id},
      expectedError: {
        _errors: [],
        optStr: {_errors: ['Expected string, received boolean']},
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (null)',
      collectionName: 'entryNoID',
      stored: null,
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {clientID},
      expectedError: {
        _errors: ['Expected object, received null'],
      },
      expectedValue: undefined,
    },
    {
      name: 'Stored value is incorrect (string)',
      collectionName: 'entryID',
      stored: 'junk',
      storedKey: '-/p/$CLIENT_ID/entryID/id/a',
      param: {clientID, id: 'a'},
      expectedError: {
        _errors: ['Expected object, received string'],
      },
      expectedValue: undefined,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(`${c.name} using ${c.collectionName}`, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;

        const fixedC = replaceClientID(c, '$CLIENT_ID', clientID);
        const {
          collectionName,
          stored,
          storedKey = stored
            ? keyFromID(collectionName, stored as PresenceEntity)
            : undefined,
          param,
          expectedError,
        } = fixedC;
        const expectedValue =
          'expectedValue' in fixedC ? fixedC.expectedValue : stored;

        if (stored !== undefined && storedKey !== undefined) {
          await r.mutate.directWrite({
            key: storedKey,
            val: stored,
          });
        }
        const {actual, error} = await r.query(async tx => {
          try {
            return {
              actual:
                collectionName === 'entryNoID'
                  ? await mustGetEntryNoID(tx, param as EntryNoID)
                  : await mustGetEntryID(tx, param as EntryID),
            };
          } catch (e) {
            if (e instanceof ZodError) {
              return {error: e.format()};
            }
            return {error: String(e)};
          }
        });

        expect(error).toEqual(expectedError);
        expect(actual).toEqual(expectedValue);
      });
    }
  }
});

suite('has', () => {
  type Case = {
    name: string;
    collectionName: 'entryNoID' | 'entryID';
    stored?: ReadonlyJSONValue | undefined;
    storedKey?: string;
    param: ReadonlyJSONValue | undefined;
    expectedHas: boolean;
  };

  const clientID = '$CLIENT_ID';
  const id = 'b';

  const cases: Case[] = [
    {
      name: 'undefined',
      collectionName: 'entryNoID',
      stored: undefined,
      param: {clientID},
      expectedHas: false,
    },
    {
      name: 'null',
      collectionName: 'entryNoID',
      stored: null,
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {clientID},
      expectedHas: true,
    },
    {
      name: 'string',
      collectionName: 'entryID',
      stored: 'junk',
      storedKey: `-/p/${clientID}/entryID/id/${id}`,
      param: {clientID, id},
      expectedHas: true,
    },
    {
      name: 'valid',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {clientID},
      expectedHas: true,
    },
    {
      name: 'valid',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {clientID, id},
      expectedHas: true,
    },
    {
      name: 'valid implicit clientID',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {},
      expectedHas: true,
    },
    {
      name: 'valid implicit clientID',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {id},
      expectedHas: true,
    },
    {
      name: 'valid no param',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: undefined,
      expectedHas: true,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(`${c.name} using ${c.collectionName}`, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {
          collectionName,
          stored,
          storedKey = stored
            ? keyFromID(collectionName, stored as PresenceEntity)
            : undefined,
          param,
          expectedHas,
        }: Case = replaceClientID(c, '$CLIENT_ID', clientID);

        if (stored !== undefined && storedKey) {
          await r.mutate.directWrite({
            key: storedKey,
            val: stored,
          });
        }
        const has = await r.query(tx => {
          if (collectionName === 'entryNoID') {
            return hasEntryNoID(tx, param as EntryNoID);
          }
          return hasEntryID(tx, param as EntryID);
        });
        expect(has).toBe(expectedHas);
      });
    }
  }
});

suite('update', () => {
  type Case = {
    name: string;
    collectionName: 'entryNoID' | 'entryID';
    stored?: ReadonlyJSONValue | undefined;
    storedKey?: string;
    param: ReadonlyJSONValue | undefined;
    expectedValue: ReadonlyJSONValue | undefined;
    expectedKey?: string;
    expectedError?: ReadonlyJSONValue;
  };

  const clientID = '$CLIENT_ID';
  const id = 'b';

  const cases: Case[] = [
    {
      name: 'stored value invalid',
      collectionName: 'entryNoID',
      stored: null,
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {},
      expectedValue: null,
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedError: {_errors: ['Expected object, received null']},
    },
    {
      name: 'stored value invalid',
      collectionName: 'entryID',
      stored: 'joke',
      storedKey: `-/p/${clientID}/entryID/id/${id}`,
      param: {id},
      expectedValue: 'joke',
      expectedKey: `-/p/${clientID}/entryID/id/${id}`,
      expectedError: {_errors: ['Expected object, received string']},
    },
    {
      name: 'stored value invalid',
      collectionName: 'entryNoID',
      stored: {clientID, id: 'should not have id', str: 'foo'},
      storedKey: `-/p/${clientID}/entryNoID/`,
      param: {clientID, str: 'bar'},
      expectedValue: {clientID, id: 'should not have id', str: 'foo'},
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedError: {_errors: ["Unrecognized key(s) in object: 'id'"]},
    },
    {
      name: 'stored value invalid',
      collectionName: 'entryID',
      stored: {clientID, str: 'missing id'},
      storedKey: `-/p/${clientID}/entryID/id/${id}`,
      param: {clientID, id, str: 'bar'},
      expectedValue: {clientID, str: 'missing id'},
      expectedKey: `-/p/${clientID}/entryID/id/${id}`,
      expectedError: {_errors: [], id: {_errors: ['Required']}},
    },
    {
      name: 'no previous value',
      collectionName: 'entryID',
      param: {clientID, str: 'foo'},
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryID/`,
    },
    {
      name: 'no previous value',
      collectionName: 'entryID',
      param: {clientID, str: 'foo', id},
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryID/id/${id}`,
    },
    {
      name: 'valid',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {clientID, str: 'bar'},
      expectedValue: {clientID, str: 'bar'},
    },
    {
      name: 'valid',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {clientID, optStr: 'opt'},
      expectedValue: {clientID, str: 'foo', optStr: 'opt'},
    },
    {
      name: 'valid',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {clientID, id, str: 'bar'},
      expectedValue: {clientID, id, str: 'bar'},
    },
    {
      name: 'valid',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {clientID, id, optStr: 'opt'},
      expectedValue: {clientID, id, str: 'foo', optStr: 'opt'},
    },

    {
      name: 'valid implicit clientID',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {str: 'bar'},
      expectedValue: {clientID, str: 'bar'},
    },
    {
      name: 'valid implicit clientID',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {optStr: 'opt'},
      expectedValue: {clientID, str: 'foo', optStr: 'opt'},
    },
    {
      name: 'valid implicit clientID',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {id, str: 'bar'},
      expectedValue: {clientID, id, str: 'bar'},
    },
    {
      name: 'valid implicit clientID',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {id, optStr: 'opt'},
      expectedValue: {clientID, id, str: 'foo', optStr: 'opt'},
    },

    {
      name: 'invalid update has wrong shape',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {clientID, str: 'bar', extra: true},
      expectedValue: {clientID, str: 'foo'},
      expectedError: {_errors: ["Unrecognized key(s) in object: 'extra'"]},
    },
    {
      name: 'invalid update has wrong type',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {clientID, id, str: false},
      expectedValue: {clientID, id, str: 'foo'},
      expectedError: {
        _errors: [],
        str: {_errors: ['Expected string, received boolean']},
      },
    },

    {
      name: 'update with wrong clientID',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {clientID: 'wrong', str: 'bar'},
      expectedValue: {clientID, str: 'foo'},
      expectedError: `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
    {
      name: 'update with wrong clientID',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {clientID: 'wrong', id, str: 'bar'},
      expectedValue: {clientID, id, str: 'foo'},
      expectedError: `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(`${c.name} using ${c.collectionName}`, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {
          collectionName,
          stored,
          storedKey = stored !== undefined
            ? keyFromID(collectionName, stored as PresenceEntity)
            : undefined,
          param,
          expectedValue,
          expectedKey = keyFromID(
            collectionName,
            expectedValue as PresenceEntity,
          ),
          expectedError,
        }: Case = replaceClientID(c, '$CLIENT_ID', clientID);

        if (stored !== undefined && storedKey !== undefined) {
          await r.mutate.directWrite({
            key: storedKey,
            val: stored,
          });
        }

        let error;

        try {
          if (collectionName === 'entryNoID') {
            await r.mutate.updateEntryNoID(param as EntryNoID);
          } else {
            await r.mutate.updateEntryID(param as EntryID);
          }
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = String(e);
          }
        }
        const actual = await r.query(tx => tx.get(expectedKey));
        expect(error).toEqual(expectedError);
        expect(actual).toEqual(expectedValue);
      });
    }
  }
});

suite('delete', () => {
  type Case = {
    name: string;
    collectionName: 'entryNoID' | 'entryID';
    stored?: ReadonlyJSONValue | undefined;
    storedKey?: string;
    param: ReadonlyJSONValue | undefined;
    expectedValue?: ReadonlyJSONValue | undefined;
    expectedKey: string;
    expectedError?: ReadonlyJSONValue;
  };

  const clientID = '$CLIENT_ID';
  const id = 'b';

  const cases: Case[] = [
    {
      name: 'previous exist',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {clientID},
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryNoID/`,
    },
    {
      name: 'previous exist',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {clientID, id},
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryID/id/${id}`,
    },

    {
      name: 'previous exist implicit clientID',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {},
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryNoID/`,
    },
    {
      name: 'previous exist implicit clientID',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {id},
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryID/id/${id}`,
    },
    {
      name: 'previous exist no param',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: undefined,
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryNoID/`,
    },

    {
      name: 'no stored value at key',
      collectionName: 'entryNoID',
      stored: undefined,
      param: {clientID},
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryNoID/`,
    },
    {
      name: 'no stored value at key',
      collectionName: 'entryID',
      stored: undefined,
      param: {clientID, id},
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryNoID/id/${id}`,
    },
    {
      name: 'no stored value at key implicit clientID',
      collectionName: 'entryNoID',
      stored: undefined,
      param: {},
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryNoID/`,
    },
    {
      name: 'no stored value at key implicit clientID',
      collectionName: 'entryID',
      stored: undefined,
      param: {id},
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryNoID/id/${id}`,
    },
    {
      name: 'no stored value at key implicit clientID',
      collectionName: 'entryNoID',
      stored: undefined,
      param: undefined,
      expectedValue: undefined,
      expectedKey: `-/p/${clientID}/entryNoID/`,
    },

    {
      name: 'wrong clientID',
      collectionName: 'entryNoID',
      stored: {clientID, str: 'foo'},
      param: {clientID: 'wrong'},
      expectedValue: {clientID, str: 'foo'},
      expectedKey: `-/p/${clientID}/entryNoID/`,
      expectedError: `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
    {
      name: 'wrong clientID',
      collectionName: 'entryID',
      stored: {clientID, id, str: 'foo'},
      param: {clientID: 'wrong', id},
      expectedValue: {clientID, id, str: 'foo'},
      expectedKey: `-/p/${clientID}/entryID/id/${id}`,
      expectedError: `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(`${c.name} using ${c.collectionName}`, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {
          collectionName,
          stored,
          storedKey = stored !== undefined
            ? keyFromID(collectionName, stored as PresenceEntity)
            : undefined,
          param,
          expectedValue,
          expectedKey,
          expectedError,
        }: Case = replaceClientID(c, '$CLIENT_ID', clientID);

        if (stored && storedKey) {
          await r.mutate.directWrite({
            key: storedKey,
            val: stored,
          });
        }

        let error;
        try {
          if (collectionName === 'entryNoID') {
            await r.mutate.deleteEntryNoID(param as EntryNoID);
          } else {
            await r.mutate.deleteEntryID(param as EntryID);
          }
        } catch (e) {
          error = String(e);
        }

        const actual = await r.query(tx => tx.get(expectedKey));

        expect(actual).toEqual(expectedValue);
        expect(error).toEqual(expectedError);
      });
    }
  }
});

suite('list', () => {
  type Case = {
    name: string;
    collectionName: 'entryNoID' | 'entryID';
    stored: Record<string, ReadonlyJSONValue>;
    param:
      | PresenceListOptions<EntryID>
      | PresenceListOptions<EntryNoID>
      | undefined;
    expectedValues: ReadonlyJSONValue[] | undefined;
    expectedError?: ReadonlyJSONValue;
  };

  const cases: Case[] = [
    {
      name: 'all',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: undefined,
      expectedValues: [
        {clientID: 'clientB', str: 'b'},
        {clientID: 'clientD', str: 'd'},
        {clientID: 'clientF', str: 'f'},
      ],
    },

    {
      name: 'all',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: undefined,
      expectedValues: [
        {clientID: 'clientB', id: 'b', str: 'b'},
        {clientID: 'clientD', id: 'd', str: 'd'},
        {clientID: 'clientF', id: 'f', str: 'f'},
      ],
    },

    {
      name: 'startAtID',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}},
      expectedValues: [
        {clientID: 'clientD', str: 'd'},
        {clientID: 'clientF', str: 'f'},
      ],
    },
    {
      name: 'startAtID',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}},
      expectedValues: [
        {clientID: 'clientD', id: 'd', str: 'd'},
        {clientID: 'clientF', id: 'f', str: 'f'},
      ],
    },
    {
      name: 'startAtID with clientID and id',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {
          clientID: 'clientB',
          id: 'b',
          str: 'b',
        },
        '-/p/clientD/entryID/id/d111': {
          clientID: 'clientD',
          id: 'd111',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d222': {
          clientID: 'clientD',
          id: 'd222',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d333': {
          clientID: 'clientD',
          id: 'd333',
          str: 'd',
        },
      },
      param: {startAtID: {clientID: 'clientD', id: 'd2'}},
      expectedValues: [
        {
          clientID: 'clientD',
          id: 'd222',
          str: 'd',
        },
        {
          clientID: 'clientD',
          id: 'd333',
          str: 'd',
        },
        {
          clientID: 'clientF',
          id: 'f',
          str: 'f',
        },
      ],
    },

    {
      name: 'startAtID and limit',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}, limit: 1},
      expectedValues: [{clientID: 'clientD', str: 'd'}],
    },
    {
      name: 'startAtID and limit',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}, limit: 1},
      expectedValues: [{clientID: 'clientD', id: 'd', str: 'd'}],
    },
    {
      name: 'startAtID with clientID and id and limit',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {
          clientID: 'clientB',
          id: 'b',
          str: 'b',
        },
        '-/p/clientD/entryID/id/d111': {
          clientID: 'clientD',
          id: 'd111',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d222': {
          clientID: 'clientD',
          id: 'd222',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d333': {
          clientID: 'clientD',
          id: 'd333',
          str: 'd',
        },
      },
      param: {startAtID: {clientID: 'clientD', id: 'd2'}, limit: 2},
      expectedValues: [
        {
          clientID: 'clientD',
          id: 'd222',
          str: 'd',
        },
        {
          clientID: 'clientD',
          id: 'd333',
          str: 'd',
        },
      ],
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(`${c.name} using ${c.collectionName}`, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {
          collectionName,
          stored,
          param,
          expectedValues,
          expectedError,
        }: Case = replaceClientID(c, '$CLIENT_ID', clientID);

        for (const [key, val] of Object.entries(stored)) {
          await r.mutate.directWrite({
            key,
            val,
          });
        }

        let error;
        let actual;
        try {
          if (collectionName === 'entryNoID') {
            actual = await r.query(tx => listEntryNoID(tx, param));
          } else {
            actual = await r.query(tx => listEntryID(tx, param));
          }
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = e;
          }
        }
        expect(error).toEqual(expectedError);
        expect(actual).toEqual(expectedValues);
      });
    }
  }
});

suite('listIDs', () => {
  type Case = {
    name: string;
    collectionName: 'entryNoID' | 'entryID';
    stored: Record<string, ReadonlyJSONValue>;
    param:
      | PresenceListOptions<EntryID>
      | PresenceListOptions<EntryNoID>
      | undefined;
    expectedValues: ReadonlyJSONValue[] | undefined;
    expectedError?: ReadonlyJSONValue;
  };

  const cases: Case[] = [
    {
      name: 'all',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: undefined,
      expectedValues: [
        {clientID: 'clientB'},
        {clientID: 'clientD'},
        {clientID: 'clientF'},
      ],
    },

    {
      name: 'all',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: undefined,
      expectedValues: [
        {clientID: 'clientB', id: 'b'},
        {clientID: 'clientD', id: 'd'},
        {clientID: 'clientF', id: 'f'},
      ],
    },

    {
      name: 'startAtID',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}},
      expectedValues: [{clientID: 'clientD'}, {clientID: 'clientF'}],
    },
    {
      name: 'startAtID',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}},
      expectedValues: [
        {clientID: 'clientD', id: 'd'},
        {clientID: 'clientF', id: 'f'},
      ],
    },
    {
      name: 'startAtID with clientID and id',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {
          clientID: 'clientB',
          id: 'b',
          str: 'b',
        },
        '-/p/clientD/entryID/id/d111': {
          clientID: 'clientD',
          id: 'd111',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d222': {
          clientID: 'clientD',
          id: 'd222',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d333': {
          clientID: 'clientD',
          id: 'd333',
          str: 'd',
        },
      },
      param: {startAtID: {clientID: 'clientD', id: 'd2'}},
      expectedValues: [
        {
          clientID: 'clientD',
          id: 'd222',
        },
        {
          clientID: 'clientD',
          id: 'd333',
        },
        {
          clientID: 'clientF',
          id: 'f',
        },
      ],
    },

    {
      name: 'startAtID and limit',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}, limit: 1},
      expectedValues: [{clientID: 'clientD'}],
    },
    {
      name: 'startAtID and limit',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}, limit: 1},
      expectedValues: [{clientID: 'clientD', id: 'd'}],
    },
    {
      name: 'startAtID with clientID and id and limit',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {
          clientID: 'clientB',
          id: 'b',
          str: 'b',
        },
        '-/p/clientD/entryID/id/d111': {
          clientID: 'clientD',
          id: 'd111',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d222': {
          clientID: 'clientD',
          id: 'd222',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d333': {
          clientID: 'clientD',
          id: 'd333',
          str: 'd',
        },
      },
      param: {startAtID: {clientID: 'clientD', id: 'd2'}, limit: 2},
      expectedValues: [
        {
          clientID: 'clientD',
          id: 'd222',
        },
        {
          clientID: 'clientD',
          id: 'd333',
        },
      ],
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(`${c.name} using ${c.collectionName}`, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {
          collectionName,
          stored,
          param,
          expectedValues,
          expectedError,
        }: Case = replaceClientID(c, '$CLIENT_ID', clientID);

        for (const [key, val] of Object.entries(stored)) {
          await r.mutate.directWrite({
            key,
            val,
          });
        }

        let error;
        let actual;
        try {
          if (collectionName === 'entryNoID') {
            actual = await r.query(tx => listIDsEntryNoID(tx, param));
          } else {
            actual = await r.query(tx => listIDsEntryID(tx, param));
          }
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = e;
          }
        }
        expect(error).toEqual(expectedError);
        expect(actual).toEqual(expectedValues);
      });
    }
  }
});

suite('listClientIDs', () => {
  type Case = {
    name: string;
    collectionName: 'entryNoID' | 'entryID';
    stored: Record<string, ReadonlyJSONValue>;
    param:
      | PresenceListOptions<EntryID>
      | PresenceListOptions<EntryNoID>
      | undefined;
    expectedValues: ReadonlyJSONValue[] | undefined;
    expectedError?: ReadonlyJSONValue;
  };

  const cases: Case[] = [
    {
      name: 'all',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: undefined,
      expectedValues: ['clientB', 'clientD', 'clientF'],
    },

    {
      name: 'all',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: undefined,
      expectedValues: ['clientB', 'clientD', 'clientF'],
    },

    {
      name: 'startAtID',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}},
      expectedValues: ['clientD', 'clientF'],
    },
    {
      name: 'startAtID',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}},
      expectedValues: ['clientD', 'clientF'],
    },
    {
      name: 'startAtID with clientID and id',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {
          clientID: 'clientB',
          id: 'b',
          str: 'b',
        },
        '-/p/clientD/entryID/id/d111': {
          clientID: 'clientD',
          id: 'd111',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d222': {
          clientID: 'clientD',
          id: 'd222',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d333': {
          clientID: 'clientD',
          id: 'd333',
          str: 'd',
        },
      },
      param: {startAtID: {clientID: 'clientD', id: 'd2'}},
      expectedValues: ['clientD', 'clientF'],
    },

    {
      name: 'startAtID and limit',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}, limit: 1},
      expectedValues: ['clientD'],
    },
    {
      name: 'startAtID and limit',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}, limit: 1},
      expectedValues: ['clientD'],
    },
    {
      name: 'startAtID with clientID and id and limit',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {
          clientID: 'clientB',
          id: 'b',
          str: 'b',
        },
        '-/p/clientD/entryID/id/d111': {
          clientID: 'clientD',
          id: 'd111',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d222': {
          clientID: 'clientD',
          id: 'd222',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d333': {
          clientID: 'clientD',
          id: 'd333',
          str: 'd',
        },
      },
      param: {startAtID: {clientID: 'clientD', id: 'd2'}, limit: 2},
      expectedValues: ['clientD', 'clientF'],
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(`${c.name} using ${c.collectionName}`, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {
          collectionName,
          stored,
          param,
          expectedValues,
          expectedError,
        }: Case = replaceClientID(c, '$CLIENT_ID', clientID);

        for (const [key, val] of Object.entries(stored)) {
          await r.mutate.directWrite({
            key,
            val,
          });
        }

        let error;
        let actual;
        try {
          if (collectionName === 'entryNoID') {
            actual = await r.query(tx => listClientIDsEntryNoID(tx, param));
          } else {
            actual = await r.query(tx => listClientIDsEntryID(tx, param));
          }
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = e;
          }
        }
        expect(error).toEqual(expectedError);
        expect(actual).toEqual(expectedValues);
      });
    }
  }
});

suite('listEntries', () => {
  type Case = {
    name: string;
    collectionName: 'entryNoID' | 'entryID';
    stored: Record<string, ReadonlyJSONValue>;
    param:
      | PresenceListOptions<EntryID>
      | PresenceListOptions<EntryNoID>
      | undefined;
    expectedValues: ReadonlyJSONValue[] | undefined;
    expectedError?: ReadonlyJSONValue;
  };

  const cases: Case[] = [
    {
      name: 'all',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: undefined,
      expectedValues: [
        [{clientID: 'clientB'}, {clientID: 'clientB', str: 'b'}],
        [{clientID: 'clientD'}, {clientID: 'clientD', str: 'd'}],
        [{clientID: 'clientF'}, {clientID: 'clientF', str: 'f'}],
      ],
    },

    {
      name: 'all',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: undefined,
      expectedValues: [
        [
          {clientID: 'clientB', id: 'b'},
          {clientID: 'clientB', id: 'b', str: 'b'},
        ],
        [
          {clientID: 'clientD', id: 'd'},
          {clientID: 'clientD', id: 'd', str: 'd'},
        ],
        [
          {clientID: 'clientF', id: 'f'},
          {clientID: 'clientF', id: 'f', str: 'f'},
        ],
      ],
    },

    {
      name: 'startAtID',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}},
      expectedValues: [
        [{clientID: 'clientD'}, {clientID: 'clientD', str: 'd'}],
        [{clientID: 'clientF'}, {clientID: 'clientF', str: 'f'}],
      ],
    },
    {
      name: 'startAtID',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}},
      expectedValues: [
        [
          {clientID: 'clientD', id: 'd'},
          {clientID: 'clientD', id: 'd', str: 'd'},
        ],
        [
          {clientID: 'clientF', id: 'f'},
          {clientID: 'clientF', id: 'f', str: 'f'},
        ],
      ],
    },
    {
      name: 'startAtID with clientID and id',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {
          clientID: 'clientB',
          id: 'b',
          str: 'b',
        },
        '-/p/clientD/entryID/id/d111': {
          clientID: 'clientD',
          id: 'd111',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d222': {
          clientID: 'clientD',
          id: 'd222',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d333': {
          clientID: 'clientD',
          id: 'd333',
          str: 'd',
        },
      },
      param: {startAtID: {clientID: 'clientD', id: 'd2'}},
      expectedValues: [
        [
          {
            clientID: 'clientD',
            id: 'd222',
          },
          {
            clientID: 'clientD',
            id: 'd222',
            str: 'd',
          },
        ],
        [
          {
            clientID: 'clientD',
            id: 'd333',
          },
          {
            clientID: 'clientD',
            id: 'd333',
            str: 'd',
          },
        ],
        [
          {
            clientID: 'clientF',
            id: 'f',
          },
          {
            clientID: 'clientF',
            id: 'f',
            str: 'f',
          },
        ],
      ],
    },

    {
      name: 'startAtID and limit',
      collectionName: 'entryNoID',
      stored: {
        '-/p/clientF/entryNoID/': {clientID: 'clientF', str: 'f'},
        '-/p/clientB/entryNoID/': {clientID: 'clientB', str: 'b'},
        '-/p/clientD/entryNoID/': {clientID: 'clientD', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}, limit: 1},
      expectedValues: [
        [{clientID: 'clientD'}, {clientID: 'clientD', str: 'd'}],
      ],
    },
    {
      name: 'startAtID and limit',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {clientID: 'clientB', id: 'b', str: 'b'},
        '-/p/clientD/entryID/id/d': {clientID: 'clientD', id: 'd', str: 'd'},
      },
      param: {startAtID: {clientID: 'clientC'}, limit: 1},
      expectedValues: [
        [
          {clientID: 'clientD', id: 'd'},
          {clientID: 'clientD', id: 'd', str: 'd'},
        ],
      ],
    },
    {
      name: 'startAtID with clientID and id and limit',
      collectionName: 'entryID',
      stored: {
        '-/p/clientF/entryID/id/f': {clientID: 'clientF', id: 'f', str: 'f'},
        '-/p/clientB/entryID/id/b': {
          clientID: 'clientB',
          id: 'b',
          str: 'b',
        },
        '-/p/clientD/entryID/id/d111': {
          clientID: 'clientD',
          id: 'd111',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d222': {
          clientID: 'clientD',
          id: 'd222',
          str: 'd',
        },
        '-/p/clientD/entryID/id/d333': {
          clientID: 'clientD',
          id: 'd333',
          str: 'd',
        },
      },
      param: {startAtID: {clientID: 'clientD', id: 'd2'}, limit: 2},
      expectedValues: [
        [
          {
            clientID: 'clientD',
            id: 'd222',
          },
          {
            clientID: 'clientD',
            id: 'd222',
            str: 'd',
          },
        ],
        [
          {
            clientID: 'clientD',
            id: 'd333',
          },
          {
            clientID: 'clientD',
            id: 'd333',
            str: 'd',
          },
        ],
      ],
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(`${c.name} using ${c.collectionName}`, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {
          collectionName,
          stored,
          param,
          expectedValues,
          expectedError,
        }: Case = replaceClientID(c, '$CLIENT_ID', clientID);

        for (const [key, val] of Object.entries(stored)) {
          await r.mutate.directWrite({
            key,
            val,
          });
        }

        let error;
        let actual;
        try {
          if (collectionName === 'entryNoID') {
            actual = await r.query(tx => listEntriesEntryNoID(tx, param));
          } else {
            actual = await r.query(tx => listEntriesEntryID(tx, param));
          }
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = e;
          }
        }
        expect(error).toEqual(expectedError);
        expect(actual).toEqual(expectedValues);
      });
    }
  }
});

type Console = {
  debug(...args: unknown[]): void;
  info(...args: unknown[]): void;
  error(...args: unknown[]): void;
};

declare const console: Console;
suite('optionalLogger', () => {
  type Case = {
    name: string;
    logger: OptionalLogger | undefined;
    expected: ((clientID: string) => unknown[]) | undefined;
  };

  let output: unknown[] | undefined = undefined;

  const cases: Case[] = [
    {
      name: 'undefined',
      logger: undefined,
      expected: undefined,
    },
    {
      name: 'empty',
      logger: {},
      expected: undefined,
    },
    {
      name: 'console',
      logger: console,
      expected: undefined,
    },
    {
      name: 'custom',
      logger: {
        debug: (...args: unknown[]) => {
          output = args;
        },
      },
      expected: clientID => [
        `no such entity {"clientID":"${clientID}"}, skipping update`,
      ],
    },
  ];

  const name = 'nnnn';

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const {update: updateEntryNoID} = generatePresence(
          name,
          entryNoID.parse,
          c.logger,
        );
        output = undefined;

        const r = f({updateEntryNoID});
        const clientID = await r.clientID;

        await r.mutate.updateEntryNoID({clientID, str: 'bar'});
        expect(output).toEqual(c.expected?.(clientID));
      });
    }
  }
});

test('undefined parse', async () => {
  (globalThis as Record<string, unknown>).process = {
    env: {
      NODE_ENV: '',
    },
  } as const;

  const name = 'nnn';
  const generated = generatePresence<EntryNoID>(name);
  const {get, list, listIDs} = generated;

  const r = new Replicache({
    name: nanoid(),
    mutators: generated,
    licenseKey: TEST_LICENSE_KEY,
  });
  const clientID = await r.clientID;

  let v = await r.query(tx => get(tx, {clientID}));
  expect(v).toBe(undefined);

  await r.mutate.set({clientID, str: 'bar'});
  await r.mutate.set({
    clientID,
    id: 'id2',
    bonk: 'baz',
  } as unknown as EntryNoID);

  v = await r.query(tx => get(tx, {clientID}));
  expect(v).toEqual({clientID, str: 'bar'});
  v = await r.query(tx =>
    get(tx, {clientID, id: 'id2'} as unknown as EntryNoID),
  );
  expect(v).toEqual({clientID, id: 'id2', bonk: 'baz'});

  const l = await r.query(tx => list(tx));
  expect(l).toEqual([
    {clientID, str: 'bar'},
    {clientID, id: 'id2', bonk: 'baz'},
  ]);

  const l2 = await r.query(tx => listIDs(tx));
  expect(l2).toEqual([{clientID}, {clientID, id: 'id2'}]);
});

test('parse key', () => {
  const name = 'foo';
  expect(parseKeyToID(name, '-/p/clientID1/foo/id/id1')).toEqual({
    clientID: 'clientID1',
    id: 'id1',
  });
  expect(parseKeyToID(name, '-/p/clientID1/foo/id/')).toEqual({
    clientID: 'clientID1',
    id: '',
  });
  expect(parseKeyToID(name, '-/p/clientID1/foo/')).toEqual({
    clientID: 'clientID1',
  });

  expect(parseKeyToID(name, '-/p/clientID1')).toBe(undefined);
  expect(parseKeyToID(name, '-/p/clientID1/')).toBe(undefined);
  expect(parseKeyToID(name, '-/p/clientID1/xxx')).toBe(undefined);
  expect(parseKeyToID(name, '-/p/clientID1/foo')).toBe(undefined);
  expect(parseKeyToID(name, '-/p/clientID1/foo/id')).toBe(undefined);
  expect(parseKeyToID(name, '-/p//')).toBe(undefined);
  expect(parseKeyToID(name, '-/p/clientID1/foo/id/id1/')).toBe(undefined);
  expect(parseKeyToID(name, '-/p/clientID1/foo/id//')).toBe(undefined);
  expect(parseKeyToID(name, '-/p/')).toBe(undefined);
  expect(parseKeyToID(name, '-/p')).toBe(undefined);
  expect(parseKeyToID(name, '-/')).toBe(undefined);
  expect(parseKeyToID(name, '-')).toBe(undefined);
  expect(parseKeyToID(name, '')).toBe(undefined);
  expect(parseKeyToID(name, 'mango')).toBe(undefined);
});

test('normalizeScanOptions', () => {
  expect(normalizeScanOptions(undefined)).undefined;
  expect(normalizeScanOptions({})).toEqual({
    startAtID: undefined,
    limit: undefined,
  });
  expect(normalizeScanOptions({limit: 123})).toEqual({
    startAtID: undefined,
    limit: 123,
  });
  expect(
    normalizeScanOptions<EntryID>({startAtID: {clientID: 'cid', id: 'a'}}),
  ).toEqual({
    startAtID: {clientID: 'cid', id: 'a'},
    limit: undefined,
  });
  expect(normalizeScanOptions({startAtID: {clientID: 'b'}})).toEqual({
    startAtID: {clientID: 'b'},
    limit: undefined,
  });
});
