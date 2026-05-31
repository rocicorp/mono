/* oxlint-disable require-await */

import {expectTypeOf, test} from 'vitest';
import {assert} from '../../shared/src/asserts.ts';
import type {ReadonlyJSONObject} from '../../shared/src/json.ts';
import type {IndexKey} from './db/index.ts';
import {Replicache} from './replicache.ts';
import type {DeepReadonly, WriteTransaction} from './transactions.ts';

function use(..._args: unknown[]) {
  // do nothing
}

// Only used for type checking
test('mutator optional args', async () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (_: WriteTransaction, x: number) => x,
      mut2: (_: WriteTransaction, x: string) => x,
      mut3: (_: WriteTransaction) => {},
      mut4: async (_: WriteTransaction) => {},
    },
  });

  const {mut, mut2, mut3, mut4} = rep.mutate;
  expectTypeOf(mut).toExtend<(x: number) => Promise<number>>();
  expectTypeOf(mut2).toExtend<(x: string) => Promise<string>>();
  expectTypeOf(mut3).toExtend<() => Promise<void>>();
  expectTypeOf(mut4).toExtend<() => Promise<void>>();

  //  @ts-expect-error: Expected 0 arguments, but got 1.ts(2554)
  await mut3(42);

  expectTypeOf(await mut3()).not.toBeNumber();

  await mut4();
  //  @ts-expect-error: Expected 0 arguments, but got 1.ts(2554)
  await mut4(42);
  expectTypeOf(await mut4()).not.toBeNumber();

  // This should be an error!
  // new Replicache({name: 'test-types-2', {
  //   mutators: {
  //     // @ts-expect-error symbol is not a JSONValue
  //     mut5: (tx: WriteTransaction, x: symbol) => {
  //       use(tx, x);
  //       return 42;
  //     },
  //   },
  // });
});

// Only used for type checking
test('Test partial JSONObject', async () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction, todo: Partial<Todo>) => {
        use(tx);
        return todo;
      },
    },
  });

  type Todo = {id: number; text: string};

  const {mut} = rep.mutate;
  await mut({});
  await mut({id: 42});
  await mut({text: 'abc'});

  expectTypeOf(mut).toBeFunction();
  expectTypeOf(mut).parameters.toExtend<[Partial<Todo>]>();

  // @ts-expect-error Type '42' has no properties in common with type 'Partial<Todo>'.ts(2559)
  await mut(42);
  // @ts-expect-error Type 'string' is not assignable to type 'number | undefined'.ts(2322)
  await mut({id: 'abc'});
});

// Only used for type checking
test('Test register param', () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (_: WriteTransaction) => {},
      mut2: async (_: WriteTransaction, x: string) => {
        expectTypeOf(x).toBeString();
      },
      mut3: async (_: WriteTransaction, x: string) => {
        expectTypeOf(x).toBeString();
      },
      mut4: async (_: WriteTransaction) => {},
    },
  });

  const {mut, mut2, mut3, mut4} = rep.mutate;
  expectTypeOf(mut).toBeFunction();
  expectTypeOf(mut).parameters.toExtend<[]>();
  expectTypeOf(mut).returns.toEqualTypeOf<Promise<void>>();

  expectTypeOf(mut2).not.toExtend<(x: number) => Promise<void>>();

  expectTypeOf(mut2).toBeFunction();
  expectTypeOf(mut2).parameters.toExtend<[string]>();
  expectTypeOf(mut2).returns.toEqualTypeOf<Promise<void>>();

  expectTypeOf(mut3).not.toExtend<() => Promise<void>>();
  expectTypeOf(mut3).toBeFunction();
  expectTypeOf(mut3).parameters.toExtend<[string]>();
  expectTypeOf(mut3).returns.toEqualTypeOf<Promise<void>>();

  // This is fine according to the rules of JS/TS
  expectTypeOf(mut4).toExtend<(x: number) => Promise<void>>();

  new Replicache({
    name: 'test-types',
    mutators: {
      // @ts-expect-error Type '(tx: WriteTransaction, a: string, b: number) =>
      //   Promise<void>' is not assignable to type '(tx: WriteTransaction,
      //   args?: any) => MaybePromise<void | JSONValue>'.ts(2322)
      mut5: async (tx: WriteTransaction, a: string, b: number) => {
        use(tx, a, b);
      },
    },
  });
});

// Only used for type checking
test('Key type for scans', async () => {
  const rep = new Replicache({
    name: 'test-types',
  });

  await rep.query(async tx => {
    for await (const k of tx.scan({indexName: 'n'}).keys()) {
      expectTypeOf(k).toExtend<IndexKey>();
      expectTypeOf(k).not.toExtend<string>();
    }

    for await (const k of tx.scan({indexName: 'n', start: {key: 's'}}).keys()) {
      expectTypeOf(k).toExtend<IndexKey>();
      expectTypeOf(k).not.toExtend<string>();
    }

    for await (const k of tx
      .scan({indexName: 'n', start: {key: ['s']}})
      .keys()) {
      expectTypeOf(k).toExtend<IndexKey>();
      expectTypeOf(k).not.toExtend<string>();
    }

    for await (const k of tx.scan({start: {key: 'p'}}).keys()) {
      expectTypeOf(k).toExtend<string>();
      // @ts-expect-error Type 'string' is not assignable to type '[string]'.ts(2322)
      const k2: [string] = k;
      use(k2);
    }

    // @ts-expect-error Type 'number' is not assignable to type 'string | undefined'.ts(2322)
    tx.scan({indexName: 'n', start: {key: ['s', 42]}});

    // @ts-expect-error Type '[string]' is not assignable to type 'string'.ts(2322)
    tx.scan({start: {key: ['s']}});
  });
});

// Only used for type checking
test('mut', async () => {
  type CustomType = {
    n: number;
    s: string;
  };

  interface CustomInterface {
    n: number;
    s: string;
  }

  type ToRecord<T> = {[P in keyof T]: T[P]};

  const rep = new Replicache({
    name: 'type-checking-only',
    mutators: {
      a: (tx: WriteTransaction) => {
        use(tx);
        return 42;
      },
      b: (tx: WriteTransaction, x: number) => {
        use(tx, x);
        return 'hi';
      },

      // Return void
      c: (tx: WriteTransaction) => {
        use(tx);
      },
      d: (tx: WriteTransaction, x: number) => {
        use(tx, x);
      },

      e: async (tx: WriteTransaction) => {
        use(tx);
        return 42;
      },
      f: async (tx: WriteTransaction, x: number) => {
        use(tx, x);
        return 'hi';
      },

      // Return void
      g: async (tx: WriteTransaction) => {
        use(tx);
      },
      h: async (tx: WriteTransaction, x: number) => {
        use(tx, x);
      },

      // // This should be flagged as an error but I need to use `any` for the
      // // arg since I need covariance and TS uses contravariance here.
      // // @ts-expect-error XXX
      // i: (tx: WriteTransaction, d: Date) =>
      // {use(tx, d);
      // },

      j: async (tx: WriteTransaction, custom: CustomType) => {
        use(tx, custom);
        expectTypeOf(custom.n).toExtend<number>();
        expectTypeOf(custom.s).toExtend<string>();
        expectTypeOf(custom.n).not.toExtend<boolean>();

        await tx.set('c', custom);
      },

      k: async (tx: WriteTransaction, custom: CustomInterface) => {
        use(tx, custom);
        expectTypeOf(custom.n).toExtend<number>();
        expectTypeOf(custom.s).toExtend<string>();
        expectTypeOf(custom.n).not.toExtend<boolean>();

        // @ts-expect-error Index signature is missing in type 'CustomInterface'
        await tx.set('c', custom);
      },

      l: async (tx: WriteTransaction, custom: ToRecord<CustomInterface>) => {
        use(tx, custom);
        expectTypeOf(custom.n).toExtend<number>();
        expectTypeOf(custom.s).toExtend<string>();
        expectTypeOf(custom.n).not.toExtend<boolean>();

        await tx.set('c', custom);
      },
    },
  });

  expectTypeOf(rep.mutate.a()).toEqualTypeOf<Promise<number>>();
  expectTypeOf(rep.mutate.b(4)).toEqualTypeOf<Promise<string>>();

  expectTypeOf(rep.mutate.c()).toEqualTypeOf<Promise<void>>();
  expectTypeOf(rep.mutate.d(2)).toEqualTypeOf<Promise<void>>();

  expectTypeOf(rep.mutate.e()).toEqualTypeOf<Promise<number>>();
  expectTypeOf(rep.mutate.f(4)).toEqualTypeOf<Promise<string>>();

  expectTypeOf(rep.mutate.g()).toEqualTypeOf<Promise<void>>();
  expectTypeOf(rep.mutate.h(2)).toEqualTypeOf<Promise<void>>();

  // @ts-expect-error Expected 1 arguments, but got 0.ts(2554)
  await rep.mutate.b();
  //@ts-expect-error Argument of type 'null' is not assignable to parameter of type 'number'.ts(2345)
  await rep.mutate.b(null);

  // @ts-expect-error Expected 1 arguments, but got 0.ts(2554)
  await rep.mutate.d();
  //@ts-expect-error Argument of type 'null' is not assignable to parameter of type 'number'.ts(2345)
  await rep.mutate.d(null);

  // @ts-expect-error Expected 1 arguments, but got 0.ts(2554)
  await rep.mutate.f();
  //@ts-expect-error Argument of type 'null' is not assignable to parameter of type 'number'.ts(2345)
  await rep.mutate.f(null);

  // @ts-expect-error Expected 1 arguments, but got 0.ts(2554)
  await rep.mutate.h();
  // @ts-expect-error Argument of type 'null' is not assignable to parameter of type 'number'.ts(2345)
  await rep.mutate.h(null);

  {
    const rep = new Replicache({
      name: 'type-checking-only',
      mutators: {},
    });
    // @ts-expect-error Property 'abc' does not exist on type 'MakeMutators<{}>'.ts(2339)
    rep.mutate.abc(43);
  }

  {
    const rep = new Replicache({
      name: 'type-checking-only',
    });
    // @ts-expect-error Property 'abc' does not exist on type 'MakeMutators<{}>'.ts(2339)
    rep.mutate.abc(1, 2, 3);
  }

  {
    const rep = new Replicache({
      name: 'type-checking-only',
    });
    // @ts-expect-error Property 'abc' does not exist on type 'MakeMutators<{}>'.ts(2339)
    rep.mutate.abc(1, 2, 3);
  }
});

// Only used for type checking
test('scan with index', async () => {
  const rep = new Replicache({
    name: 'scan-with-index',
  });

  await rep.query(async tx => {
    expectTypeOf(await tx.scan({indexName: 'a'}).keys().toArray()).toExtend<
      IndexKey[]
    >();

    let indexKeys: IndexKey[] = await tx
      .scan({indexName: 'a'})
      .keys()
      .toArray();
    indexKeys = await tx.scan({indexName: 'a', prefix: 'a'}).keys().toArray();
    use(indexKeys);

    // @ts-expect-error Cannot convert Index[] to string[]
    (await tx.scan({indexName: 'a'}).keys().toArray()) as string[];
  });
});

// Only used for type checking
test('scan without index', async () => {
  const rep = new Replicache({
    name: 'scan-with-index',
  });

  await rep.query(async tx => {
    expectTypeOf(await tx.scan().keys().toArray()).toExtend<string[]>();
    expectTypeOf(await tx.scan({}).keys().toArray()).toExtend<string[]>();

    let indexKeys: string[] = await tx.scan({}).keys().toArray();
    indexKeys = await tx.scan({prefix: 'a'}).keys().toArray();
    use(indexKeys);

    // @ts-expect-error Cannot convert string[] to IndexKey[]
    (await tx.scan({}).keys().toArray()) as IndexKey[];
  });
});

// Only used for type checking
test('mutator return read only', async () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction, x: {y: number}) => {
        use(tx);
        return x as {readonly y: number};
      },
      mut2: async (tx: WriteTransaction, x: {readonly y: number}) => {
        use(tx);
        return x as {y: number};
      },
      mut3: (tx: WriteTransaction, x: Array<number>) => {
        use(tx);
        return x as ReadonlyArray<number>;
      },
      mut4: (tx: WriteTransaction, x: ReadonlyArray<number>) => {
        use(tx);
        return x as Array<number>;
      },
    },
  });

  expectTypeOf(rep.mutate.mut).toExtend<
    (x: {y: number}) => Promise<{readonly y: number}>
  >();
  expectTypeOf(rep.mutate.mut2).toExtend<
    (x: {readonly y: number}) => Promise<{y: number}>
  >();
  expectTypeOf(rep.mutate.mut3).toExtend<
    (x: number[]) => Promise<ReadonlyArray<number>>
  >();
  expectTypeOf(rep.mutate.mut4).toExtend<
    (x: ReadonlyArray<number>) => Promise<number[]>
  >();
});

test('Allowing undefined in JSONObject', async () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: (tx: WriteTransaction, x: ReadonlyJSONObject) => {
        use(tx);
        use(x);
      },
    },
  });
  expectTypeOf<{a: undefined}>().toExtend<ReadonlyJSONObject>();
  await rep.mutate.mut({a: undefined});
});

test('Parameterized get', async () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        const p = await tx.get<{x: string}>('x');
        expectTypeOf(p).toExtend<{x: string} | undefined>();
        use(p);
      },
    },
  });
  await rep.query(async tx => {
    const p = await tx.get<{x: string}>('x');
    expectTypeOf(p).toExtend<{x: string} | undefined>();
    use(p);
  });
});

test('Parameterized get invalid types', async () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        expectTypeOf(await tx.get<{x: string}>('x')).toExtend<
          {x: string} | undefined
        >();
        // @ts-expect-error Type 'string' is not assignable to type 'number'.ts(2322)
        const p: {x: number} | undefined = await tx.get<{x: string}>('x');
        use(p);
      },
    },
  });
  await rep.query(async tx => {
    expectTypeOf(await tx.get<{x: string}>('x')).toExtend<
      {x: string} | undefined
    >();
    // @ts-expect-error Type 'string' is not assignable to type 'number'.ts(2322)
    const p: {x: number} | undefined = await tx.get<{x: string}>('x');
    use(p);
  });
});

test('Parameterized get deep read only object/array', async () => {
  type T = {x: number[]};
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        const v = await tx.get<T>('x');
        assert(v, 'Expected value for key "x" to be defined');
        // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
        v.x = [42];
        // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
        v.x[0] = 42;
      },
    },
  });
  await rep.query(async tx => {
    const v = await tx.get<T>('x');
    assert(v, 'Expected value for key "x" to be defined');
    // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
    v.x = [42];
    // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
    v.x[0] = 42;
  });
});

test('Parameterized scan.values', async () => {
  type V = {x: number};
  type DeepV = DeepReadonly<V>;
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        for await (const v of tx.scan<V>()) {
          expectTypeOf(v).toExtend<DeepV>();
        }

        for await (const v of tx.scan<V>().values()) {
          expectTypeOf(v).toExtend<DeepV>();
        }

        const vs = await tx.scan<V>().values().toArray();
        expectTypeOf(vs).toExtend<DeepV[]>();

        const vs2 = await tx.scan<V>().toArray();
        expectTypeOf(vs2).toExtend<DeepV[]>();
      },
    },
  });

  await rep.query(async tx => {
    for await (const v of tx.scan<V>()) {
      expectTypeOf(v).toExtend<DeepV>();
    }

    for await (const v of tx.scan<V>().values()) {
      expectTypeOf(v).toExtend<DeepV>();
    }

    const vs: V[] = await tx.scan<V>().values().toArray();
    expectTypeOf(vs).toExtend<DeepV[]>();

    const vs2: V[] = await tx.scan<V>().toArray();
    expectTypeOf(vs2).toExtend<DeepV[]>();
  });
});

test('Parameterized index scan.values', async () => {
  type V = {x: number};
  type DeepV = DeepReadonly<V>;
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        for await (const v of tx.scan<V>({indexName: 'x'})) {
          expectTypeOf(v).toExtend<DeepV>();
        }

        for await (const v of tx.scan<V>({indexName: 'x'}).values()) {
          expectTypeOf(v).toExtend<DeepV>();
        }

        const vs = await tx.scan<V>({indexName: 'x'}).values().toArray();
        expectTypeOf(vs).toExtend<DeepV[]>();

        const vs2 = await tx.scan<V>({indexName: 'x'}).toArray();
        expectTypeOf(vs2).toExtend<DeepV[]>();
      },
    },
  });

  await rep.query(async tx => {
    for await (const v of tx.scan<V>()) {
      expectTypeOf(v).toExtend<DeepV>();
    }

    for await (const v of tx.scan<V>().values()) {
      expectTypeOf(v).toExtend<DeepV>();
    }

    const vs: V[] = await tx.scan<V>().values().toArray();
    expectTypeOf(vs).toExtend<DeepV[]>();

    const vs2: V[] = await tx.scan<V>().toArray();
    expectTypeOf(vs2).toExtend<DeepV[]>();
  });
});

test('Parameterized scan.values invalid types', async () => {
  type V = {x: number};
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        for await (const v of tx.scan<V>()) {
          // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
          const v2: {x: string} = v;
          use(v2);
        }

        for await (const v of tx.scan<V>().values()) {
          // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
          const v2: {x: string} = v;
          use(v2);
        }

        // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
        const vs: {x: string}[] = await tx.scan<V>().toArray();
        use(vs);

        // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
        const vs2: {x: string}[] = await tx.scan<V>().values().toArray();
        use(vs2);
      },
    },
  });

  await rep.query(async tx => {
    for await (const v of tx.scan<V>()) {
      expectTypeOf(v).toExtend<{readonly x: number}>();
      // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
      const v2: {x: string} = v;
      use(v2);
    }

    for await (const v of tx.scan<V>().values()) {
      expectTypeOf(v).toExtend<{readonly x: number}>();
      // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
      const v2: {x: string} = v;
      use(v2);
    }

    // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
    const vs: {x: string}[] = await tx.scan<V>().toArray();
    use(vs);

    // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
    const vs2: {x: string}[] = await tx.scan<V>().values().toArray();
    use(vs2);
  });
});

test('Parameterized scan.values deep read only object/array', async () => {
  type V = {x: number[]};
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        for await (const v of tx.scan<V>()) {
          // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
          v.x = [42];
          // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
          v.x[0] = 42;
        }

        for await (const v of tx.scan<V>().values()) {
          // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
          v.x = [42];
          // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
          v.x[0] = 42;
        }

        const vs = await tx.scan<V>().toArray();
        // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
        vs[0].x = [42];
        // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
        vs[0].x[0] = 42;

        const vs2 = await tx.scan<V>().values().toArray();
        // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
        vs2[0].x = [42];
        // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
        vs2[0].x[0] = 42;
      },
    },
  });

  await rep.query(async tx => {
    for await (const v of tx.scan<V>()) {
      expectTypeOf(v).toExtend<DeepReadonly<V>>();
      // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
      v.x = [42];
      // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
      v.x[0] = 42;
    }

    for await (const v of tx.scan<V>().values()) {
      // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
      v.x = [42];
      // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
      v.x[0] = 42;
    }

    const vs = await tx.scan<V>().toArray();
    // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
    vs[0].x = [42];
    // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
    vs[0].x[0] = 42;

    const vs2 = await tx.scan<V>().values().toArray();
    // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
    vs2[0].x = [42];
    // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
    vs2[0].x[0] = 42;
  });
});

test('Parameterized scan.entries', async () => {
  type V = {x: number};
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        for await (const e of tx.scan<V>().entries()) {
          expectTypeOf(e).toExtend<readonly [string, {readonly x: number}]>();
        }

        const es = await tx.scan<V>().entries().toArray();
        expectTypeOf(es).toExtend<
          (readonly [string, {readonly x: number}])[]
        >();
      },
    },
  });

  await rep.query(async tx => {
    for await (const v of tx.scan<V>().entries()) {
      expectTypeOf(v).toExtend<readonly [string, {readonly x: number}]>();
    }

    const es = await tx.scan<V>().entries().toArray();
    expectTypeOf(es).toExtend<(readonly [string, {readonly x: number}])[]>();
  });
});

test('Parameterized index scan.entries', async () => {
  type V = {x: number};
  type EntryDeepV = readonly [IndexKey, DeepReadonly<{x: number}>];
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        for await (const e of tx.scan<V>({indexName: 'x'}).entries()) {
          expectTypeOf(e).toExtend<EntryDeepV>();
        }

        const es = await tx.scan<V>({indexName: 'x'}).entries().toArray();
        expectTypeOf(es).toExtend<EntryDeepV[]>();
      },
    },
  });

  await rep.query(async tx => {
    for await (const v of tx.scan<V>({indexName: 'x'}).entries()) {
      expectTypeOf(v).toExtend<EntryDeepV>();
    }

    const es = await tx.scan<V>({indexName: 'x'}).entries().toArray();
    expectTypeOf(es).toExtend<EntryDeepV[]>();
  });
});

test('Parameterized scan.entries invalid types', async () => {
  type V = {x: number};
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        for await (const e of tx.scan<V>().entries()) {
          // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
          const v: readonly [string, {readonly x: string}] = e;
          use(v);
        }

        // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
        const es: (readonly [string, {readonly x: string}])[] = await tx
          .scan<V>()
          .entries()
          .toArray();
        use(es);
      },
    },
  });

  await rep.query(async tx => {
    expectTypeOf(await tx.scan<V>().entries().toArray()).toExtend<
      (readonly [string, {readonly x: number}])[]
    >();
    for await (const e of tx.scan<V>().entries()) {
      // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
      const v: readonly [string, {x: string}] = e;
      use(v);
    }

    // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
    const es: (readonly [string, {readonly x: string}])[] = await tx
      .scan<V>()
      .entries()
      .toArray();
    use(es);
  });
});

test('Parameterized scan.entries deep read only object/array', async () => {
  type V = {x: number[]};
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        for await (const e of tx.scan<V>().entries()) {
          const [k, v] = e;
          use(k);
          // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
          v.x = [42];
          // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
          v.x[0] = 42;
        }

        const es = await tx.scan<V>().entries().toArray();
        // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
        es[0][1].x = [42];
        // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
        es[0][1].x[0] = 42;
      },
    },
  });

  await rep.query(async tx => {
    for await (const e of tx.scan<V>().entries()) {
      expectTypeOf(e).toExtend<readonly [string, DeepReadonly<V>]>();
      const [k, v] = e;
      use(k);
      // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
      v.x = [42];
      // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
      v.x[0] = 42;
    }

    const es = await tx.scan<V>().entries().toArray();
    // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
    es[0][1].x = [42];
    // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
    es[0][1].x[0] = 42;
  });
});
