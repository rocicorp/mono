import {expect, expectTypeOf, test} from 'vitest';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {
  AnyQueryBuilder,
  HumanReadable,
  QueryBuilder,
  QueryReturn,
} from '../../../zql/src/query/query-builder.ts';
import {relationships} from './relationship-builder.ts';
import {clientSchemaFrom, createSchema} from './schema-builder.ts';
import {boolean, json, number, string, table} from './table-builder.ts';

const mockQueryInst = {
  select() {
    return this;
  },
  materialize() {
    return {
      get() {
        return this;
      },
    };
  },
  sub() {
    return this;
  },
  related() {
    return this;
  },
  where() {
    return this;
  },
  start() {
    return this;
  },
  one() {
    return this;
  },
  run() {
    return this;
  },
};

function mockQuery<
  T extends keyof S['tables'] & string,
  S extends Schema,
>(): QueryBuilder<T, S> {
  return mockQueryInst as unknown as QueryBuilder<T, S>;
}

function run<Q extends AnyQueryBuilder>(
  _q: Q,
): Promise<HumanReadable<QueryReturn<Q>>> {
  return {} as unknown as Promise<HumanReadable<QueryReturn<Q>>>;
}

test('building a schema', async () => {
  const user = table('user')
    .columns({
      id: string(),
      name: string(),
      recruiterId: number(),
    })
    .primaryKey('id');

  const issue = table('issue')
    .columns({
      id: string(),
      title: string(),
      ownerId: number(),
    })
    .primaryKey('id');

  const issueLabel = table('issueLabel')
    .columns({
      issueId: number(),
      labelId: number(),
    })
    .primaryKey('issueId', 'labelId');

  const label = table('label')
    .columns({
      id: number(),
      name: string(),
    })
    .primaryKey('id');

  const issueRelationships = relationships(issue, ({many, one}) => ({
    owner: one({
      sourceField: ['ownerId'],
      destField: ['id'],
      destSchema: user,
    }),
    labels: many(
      {
        sourceField: ['id'],
        destField: ['issueId'],
        destSchema: issueLabel,
      },
      {
        sourceField: ['labelId'],
        destField: ['id'],
        destSchema: label,
      },
    ),
  }));

  const userRelationships = relationships(user, ({one, many}) => ({
    recruiter: one({
      sourceField: ['id'],
      destField: ['recruiterId'],
      destSchema: user,
    }),
    ownedIssues: many({
      sourceField: ['id'],
      destField: ['ownerId'],
      destSchema: issue,
    }),
  }));

  const labelRelationships = relationships(label, ({many}) => ({
    issues: many(
      {
        sourceField: ['id'],
        destField: ['labelId'],
        destSchema: issueLabel,
      },
      {
        sourceField: ['issueId'],
        destField: ['id'],
        destSchema: issue,
      },
    ),
  }));

  const schema = createSchema({
    tables: [user, issue, issueLabel, label],
    relationships: [userRelationships, issueRelationships, labelRelationships],
  });

  const q = mockQuery<'user', typeof schema>();
  const iq = mockQuery<'issue', typeof schema>();
  const r = await run(
    q
      .related('recruiter', q => q.related('recruiter', q => q.one()).one())
      .one(),
  );

  expectTypeOf(r).toEqualTypeOf<
    | {
        readonly id: string;
        readonly name: string;
        readonly recruiterId: number;
        readonly recruiter:
          | {
              readonly id: string;
              readonly name: string;
              readonly recruiterId: number;
              readonly recruiter:
                | {
                    readonly id: string;
                    readonly name: string;
                    readonly recruiterId: number;
                  }
                | undefined;
            }
          | undefined;
      }
    | undefined
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  >({} as any);

  // recruiter is a singular relationship
  expectTypeOf(await run(q.related('recruiter'))).toEqualTypeOf<
    {
      readonly id: string;
      readonly name: string;
      readonly recruiterId: number;
      readonly recruiter:
        | {
            readonly id: string;
            readonly name: string;
            readonly recruiterId: number;
          }
        | undefined;
    }[]
  >();

  // recruiter is a singular relationship
  expectTypeOf(await run(q.related('recruiter', q => q))).toEqualTypeOf<
    {
      readonly id: string;
      readonly name: string;
      readonly recruiterId: number;
      readonly recruiter:
        | {
            readonly id: string;
            readonly name: string;
            readonly recruiterId: number;
          }
        | undefined;
    }[]
  >();

  const id1 = await run(
    iq.related('owner', q => q.related('ownedIssues', q => q.where('id', '1'))),
  );
  expectTypeOf(id1).toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly ownerId: number;
      readonly owner:
        | {
            readonly id: string;
            readonly name: string;
            readonly recruiterId: number;
            readonly ownedIssues: readonly {
              readonly id: string;
              readonly title: string;
              readonly ownerId: number;
            }[];
          }
        | undefined;
    }[]
  >({} as never);

  const id = await run(iq.related('labels'));
  expectTypeOf(id).toEqualTypeOf<
    {
      readonly id: string;
      readonly title: string;
      readonly ownerId: number;
      readonly labels: readonly {
        readonly id: number;
        readonly name: string;
      }[];
    }[]
  >();

  const lq = mockQuery<'label', typeof schema>();
  const ld = await run(lq.related('issues'));
  expectTypeOf(ld).toEqualTypeOf<
    {
      readonly id: number;
      readonly name: string;
      readonly issues: readonly {
        readonly id: string;
        readonly title: string;
        readonly ownerId: number;
      }[];
    }[]
  >();
});

test('too many relationships', () => {
  function makeTable<const N extends string>(name: N) {
    return table(name)
      .columns({
        id: string(),
        next: string(),
      })
      .primaryKey('id');
  }

  const a = makeTable('a');
  const b = makeTable('b');
  const c = makeTable('c');
  const d = makeTable('d');
  const e = makeTable('e');
  const f = makeTable('f');
  const g = makeTable('g');
  const h = makeTable('h');
  const i = makeTable('i');
  const j = makeTable('j');
  const k = makeTable('k');
  const l = makeTable('l');
  const m = makeTable('m');
  const n = makeTable('n');
  const o = makeTable('o');
  const p = makeTable('p');
  const qt = makeTable('q');
  const r = makeTable('r');
  const s = makeTable('s');
  const t = makeTable('t');
  const u = makeTable('u');
  const v = makeTable('v');
  const w = makeTable('w');
  const x = makeTable('x');
  const y = makeTable('y');
  const z = makeTable('z');

  const schema = createSchema({
    tables: [
      a,
      b,
      c,
      d,
      e,
      f,
      g,
      h,
      i,
      j,
      k,
      l,
      m,
      n,
      o,
      p,
      qt,
      r,
      s,
      t,
      u,
      v,
      w,
      x,
      y,
      z,
    ],
    relationships: [
      relationships(a, ({one}) => ({
        toB: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: b,
        }),
        toC: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: c,
        }),
        toD: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: d,
        }),
        toE: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: e,
        }),
        toF: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: f,
        }),
        toG: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: g,
        }),
        toH: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: h,
        }),
        toI: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: i,
        }),
        toJ: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: j,
        }),
        toK: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: k,
        }),
        toL: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: l,
        }),
        toM: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: m,
        }),
        toN: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: n,
        }),
        toO: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: o,
        }),
        toP: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: p,
        }),
        toQ: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: qt,
        }),
        toR: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: r,
        }),
        toS: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: s,
        }),
        toT: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: t,
        }),
        toU: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: u,
        }),
        toV: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: v,
        }),
        toW: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: w,
        }),
        toX: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: x,
        }),
        toY: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: y,
        }),
        toZ: one({
          sourceField: ['id'],
          destField: ['next'],
          destSchema: z,
        }),
      })),
    ],
  });

  const q = mockQuery<'a', typeof schema>();
  const q2 = q
    .related('toB')
    .related('toC')
    .related('toD')
    .related('toE')
    .related('toF')
    .related('toG')
    .related('toH')
    .related('toI')
    .related('toJ')
    .related('toK')
    .related('toL')
    .related('toM')
    .related('toN')
    .related('toO')
    .related('toP')
    .related('toQ')
    .related('toR')
    .related('toS')
    .related('toT')
    .related('toU')
    .related('toV')
    .related('toW')
    .related('toX')
    .related('toY')
    .related('toZ');

  // Before this commit the below line would generate 2 TS errors. One for the type number
  // but more importantly it used to raise:
  // TS2589: Type instantiation is excessively deep and possibly infinite

  // @ts-expect-error type 'number' does not satisfy the constraint
  expectTypeOf(q2).toEqualTypeOf<123>();
});

test('alternate db names', () => {
  const user = table('user')
    .from('users')
    .columns({
      id: string().from('user_id'),
      name: string().from('user_name'),
      recruiterId: number().from('user_recruiter_id'),
    })
    .primaryKey('id');

  expect(user.build()).toMatchInlineSnapshot(`
    {
      "columns": {
        "id": {
          "customType": null,
          "optional": false,
          "serverName": "user_id",
          "type": "string",
        },
        "name": {
          "customType": null,
          "optional": false,
          "serverName": "user_name",
          "type": "string",
        },
        "recruiterId": {
          "customType": null,
          "optional": false,
          "serverName": "user_recruiter_id",
          "type": "number",
        },
      },
      "name": "user",
      "primaryKey": [
        "id",
      ],
      "serverName": "users",
    }
  `);

  const foo = table('foo')
    .from('public.fooz')
    .columns({
      bar: string().from('baz'),
      baz: string().from('boo'),
      boo: number().from('bar'),
    })
    .primaryKey('bar');

  expect(foo.build()).toMatchInlineSnapshot(`
    {
      "columns": {
        "bar": {
          "customType": null,
          "optional": false,
          "serverName": "baz",
          "type": "string",
        },
        "baz": {
          "customType": null,
          "optional": false,
          "serverName": "boo",
          "type": "string",
        },
        "boo": {
          "customType": null,
          "optional": false,
          "serverName": "bar",
          "type": "number",
        },
      },
      "name": "foo",
      "primaryKey": [
        "bar",
      ],
      "serverName": "fooz",
    }
  `);
});

test('conflicting column names', () => {
  const user = table('user')
    .from('users')
    .columns({
      a: string().from('b'),
      b: string().from('c'),
      c: string(),
      recruiterId: number().from('user_recruiter_id'),
    })
    .primaryKey('a');

  expect(() => user.build()).toThrowErrorMatchingInlineSnapshot(
    `[Error: Table "user" has multiple columns referencing "c"]`,
  );
});

test('schema with conflicting table names', () => {
  const foo = table('foo').from('bar').columns({a: string()}).primaryKey('a');
  const bar = table('bar').columns({a: string()}).primaryKey('a');

  expect(() =>
    createSchema({tables: [foo, bar]}),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: Multiple tables reference the name "bar"]`,
  );
});

// Use JSON.stringify in expectations to preserve / verify key order.
const stringify = (o: unknown) => JSON.stringify(o, null, 2);

test('clientSchemaFrom', () => {
  const schema = createSchema({
    tables: [
      table('issue')
        .from('issues')
        .columns({
          id: string(),
          title: string(),
          description: string(),
          closed: boolean(),
          ownerId: string().from('owner_id').optional(),
        })
        .primaryKey('id'),
      table('comment')
        .from('comments')
        .columns({
          id: string().from('comment_id'),
          issueId: string().from('the_issue_id'), // verify sorting by serverName
          description: string(),
        })
        .primaryKey('id'),
      table('noMappings')
        .columns({
          id: string(),
          description: string(),
        })
        .primaryKey('id'),
    ],
  });

  expect(stringify(clientSchemaFrom(schema))).toMatchInlineSnapshot(`
    "{
      "clientSchema": {
        "tables": {
          "comments": {
            "columns": {
              "comment_id": {
                "type": "string"
              },
              "description": {
                "type": "string"
              },
              "the_issue_id": {
                "type": "string"
              }
            },
            "primaryKey": [
              "comment_id"
            ]
          },
          "issues": {
            "columns": {
              "closed": {
                "type": "boolean"
              },
              "description": {
                "type": "string"
              },
              "id": {
                "type": "string"
              },
              "owner_id": {
                "type": "string"
              },
              "title": {
                "type": "string"
              }
            },
            "primaryKey": [
              "id"
            ]
          },
          "noMappings": {
            "columns": {
              "description": {
                "type": "string"
              },
              "id": {
                "type": "string"
              }
            },
            "primaryKey": [
              "id"
            ]
          }
        }
      },
      "hash": "lw7fbb20qmea"
    }"
  `);
});

test('array column', () => {
  const schema = createSchema({
    tables: [
      table('issue')
        .from('issues')
        .columns({
          id: string(),
          stringArray: json<string[]>(),
          numberArray: json<number[]>(),
          booleanArray: json<boolean[]>(),
          jsonArray: json(),
          enumArray: json<('A' | 'B')[]>(),
        })
        .primaryKey('id'),
    ],
  });

  expect(stringify(clientSchemaFrom(schema))).toMatchInlineSnapshot(`
    "{
      "clientSchema": {
        "tables": {
          "issues": {
            "columns": {
              "booleanArray": {
                "type": "json"
              },
              "enumArray": {
                "type": "json"
              },
              "id": {
                "type": "string"
              },
              "jsonArray": {
                "type": "json"
              },
              "numberArray": {
                "type": "json"
              },
              "stringArray": {
                "type": "json"
              }
            },
            "primaryKey": [
              "id"
            ]
          }
        }
      },
      "hash": "39k26n5eek28e"
    }"
  `);
});
