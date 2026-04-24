/* oxlint-disable require-await */

import {assertType, test} from 'vitest';
import {assert} from '../../shared/src/asserts.ts';
import type {ReadonlyJSONObject} from '../../shared/src/json.ts';
import type {IndexKey} from './db/index.ts';
import {Replicache} from './replicache.ts';
import type {DeepReadonly, WriteTransaction} from './transactions.ts';

function use(..._args: unknown[]) {
  // do nothing
}

test('mutator optional args', async () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction, x: number) => {
        use(tx);
        return x;
      },
      mut2: (tx: WriteTransaction, x: string) => {
        use(tx);
        return x;
      },
      mut3: (tx: WriteTransaction) => {
        use(tx);
      },
      mut4: async (tx: WriteTransaction) => {
        use(tx);
      },
    },
  });

  const {mut, mut2, mut3, mut4} = rep.mutate;

  assertType<number>(await mut(42));
  assertType<string>(await mut2('s'));
  assertType<Promise<void>>(mut3());
  assertType<Promise<void>>(mut4());

  //  @ts-expect-error: Expected 0 arguments, but got 1.ts(2554)
  await mut3(42);
  //  @ts-expect-error: Type 'void' is not assignable to type 'number'.ts(2322)
  assertType<number>(await mut3());

  //  @ts-expect-error: Expected 0 arguments, but got 1.ts(2554)
  await mut4(42);
  //  @ts-expect-error: Type 'void' is not assignable to type 'number'.ts(2322)
  assertType<number>(await mut4());
});

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
  assertType<Promise<Partial<Todo>>>(mut({}));
  await mut({id: 42});
  await mut({text: 'abc'});

  // @ts-expect-error Type '42' has no properties in common with type 'Partial<Todo>'.ts(2559)
  await mut(42);
  // @ts-expect-error Type 'string' is not assignable to type 'number | undefined'.ts(2322)
  await mut({id: 'abc'});
});

test('Test register param', () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        use(tx);
      },
      mut2: async (tx: WriteTransaction, x: string) => {
        use(tx, x);
      },
      mut3: async (tx: WriteTransaction, x: string) => {
        use(tx, x);
      },
      mut4: async (tx: WriteTransaction) => {
        use(tx);
      },
    },
  });

  assertType<() => Promise<void>>(rep.mutate.mut);

  // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
  assertType<(x: number) => Promise<void>>(rep.mutate.mut2);

  // @ts-expect-error Type '(args: string) => Promise<void>' is not assignable to type '() => Promise<void>'.ts(2322)
  assertType<() => Promise<void>>(rep.mutate.mut3);

  // This is fine according to the rules of JS/TS
  assertType<(x: number) => Promise<void>>(rep.mutate.mut4);

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

test('Key type for scans', async () => {
  const rep = new Replicache({
    name: 'test-types',
  });

  await rep.query(async tx => {
    for await (const k of tx.scan({indexName: 'n'}).keys()) {
      assertType<IndexKey>(k);
      // @ts-expect-error Type '[secondary: string, primary?: string | undefined]' is not assignable to type 'string'.ts(2322)
      const k2: string = k;
      use(k2);
    }

    for await (const k of tx.scan({indexName: 'n', start: {key: 's'}}).keys()) {
      assertType<IndexKey>(k);
      // @ts-expect-error Type '[secondary: string, primary?: string | undefined]' is not assignable to type 'string'.ts(2322)
      const k2: string = k;
      use(k2);
    }

    for await (const k of tx
      .scan({indexName: 'n', start: {key: ['s']}})
      .keys()) {
      assertType<IndexKey>(k);
      // @ts-expect-error Type '[secondary: string, primary?: string | undefined]' is not assignable to type 'string'.ts(2322)
      const k2: string = k;
      use(k2);
    }

    for await (const k of tx.scan({start: {key: 'p'}}).keys()) {
      assertType<string>(k);
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

      j: async (tx: WriteTransaction, custom: CustomType) => {
        use(tx, custom);
        custom.n as number;
        custom.s as string;
        // @ts-expect-error xxx
        custom.n as boolean;

        await tx.set('c', custom);
      },

      k: async (tx: WriteTransaction, custom: CustomInterface) => {
        use(tx, custom);
        custom.n as number;
        custom.s as string;
        // @ts-expect-error xxx
        custom.n as boolean;

        // @ts-expect-error Index signature is missing in type 'CustomInterface'
        await tx.set('c', custom);
      },

      l: async (tx: WriteTransaction, custom: ToRecord<CustomInterface>) => {
        use(tx, custom);
        custom.n as number;
        custom.s as string;
        // @ts-expect-error xxx
        custom.n as boolean;

        await tx.set('c', custom);
      },
    },
  });

  void (rep.mutate.a() satisfies Promise<number>);
  void (rep.mutate.b(4) satisfies Promise<string>);

  void (rep.mutate.c() satisfies Promise<void>);
  void (rep.mutate.d(2) satisfies Promise<void>);

  void (rep.mutate.e() satisfies Promise<number>);
  void (rep.mutate.f(4) satisfies Promise<string>);

  void (rep.mutate.g() satisfies Promise<void>);
  void (rep.mutate.h(2) satisfies Promise<void>);

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

test('scan with index', async () => {
  const rep = new Replicache({
    name: 'scan-with-index',
  });

  await rep.query(async tx => {
    assertType<IndexKey[]>(await tx.scan({indexName: 'a'}).keys().toArray());
    assertType<IndexKey[]>(
      await tx.scan({indexName: 'a', prefix: 'a'}).keys().toArray(),
    );

    // @ts-expect-error Cannot convert Index[] to string[]
    (await tx.scan({indexName: 'a'}).keys().toArray()) as string[];
  });
});

test('scan without index', async () => {
  const rep = new Replicache({
    name: 'scan-with-index',
  });

  await rep.query(async tx => {
    assertType<string[]>(await tx.scan().keys().toArray());
    assertType<string[]>(await tx.scan({prefix: 'a'}).keys().toArray());

    // @ts-expect-error Cannot convert string[] to IndexKey[]
    (await tx.scan({}).keys().toArray()) as IndexKey[];
  });
});

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

  assertType<Promise<{readonly y: number}>>(rep.mutate.mut({y: 1}));
  assertType<Promise<{y: number}>>(rep.mutate.mut2({y: 1}));
  assertType<Promise<ReadonlyArray<number>>>(rep.mutate.mut3([1, 2]));
  assertType<Promise<Array<number>>>(rep.mutate.mut4([1, 2]));
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
  assertType<Promise<void>>(rep.mutate.mut({a: undefined}));
});

test('Parameterized get', async () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        assertType<{x: string} | undefined>(await tx.get<{x: string}>('x'));
      },
    },
  });
  await rep.query(async tx => {
    assertType<{x: string} | undefined>(await tx.get<{x: string}>('x'));
  });
});

test('Parameterized get invalid types', async () => {
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        assertType<{x: string} | undefined>(await tx.get<{x: string}>('x'));
        // @ts-expect-error Type 'string' is not assignable to type 'number'.ts(2322)
        const p: {x: number} | undefined = await tx.get<{x: string}>('x');
        use(p);
      },
    },
  });
  await rep.query(async tx => {
    assertType<{x: string} | undefined>(await tx.get<{x: string}>('x'));
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
        assertType<DeepReadonly<T>>(v);
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
    assertType<DeepReadonly<T>>(v);
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
          assertType<DeepV>(v);
        }

        for await (const v of tx.scan<V>().values()) {
          assertType<DeepV>(v);
        }

        const vs = await tx.scan<V>().values().toArray();
        assertType<DeepV[]>(vs);

        const vs2 = await tx.scan<V>().toArray();
        assertType<DeepV[]>(vs2);
      },
    },
  });

  await rep.query(async tx => {
    for await (const v of tx.scan<V>()) {
      assertType<DeepV>(v);
    }

    for await (const v of tx.scan<V>().values()) {
      assertType<DeepV>(v);
    }

    const vs: V[] = await tx.scan<V>().values().toArray();
    assertType<DeepV[]>(vs);

    const vs2: V[] = await tx.scan<V>().toArray();
    assertType<DeepV[]>(vs2);
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
          assertType<DeepV>(v);
        }

        for await (const v of tx.scan<V>({indexName: 'x'}).values()) {
          assertType<DeepV>(v);
        }

        const vs = await tx.scan<V>({indexName: 'x'}).values().toArray();
        assertType<DeepV[]>(vs);

        const vs2 = await tx.scan<V>({indexName: 'x'}).toArray();
        assertType<DeepV[]>(vs2);
      },
    },
  });

  await rep.query(async tx => {
    for await (const v of tx.scan<V>()) {
      assertType<DeepV>(v);
    }

    for await (const v of tx.scan<V>().values()) {
      assertType<DeepV>(v);
    }

    const vs: V[] = await tx.scan<V>().values().toArray();
    assertType<DeepV[]>(vs);

    const vs2: V[] = await tx.scan<V>().toArray();
    assertType<DeepV[]>(vs2);
  });
});

test('Parameterized scan.values invalid types', async () => {
  type V = {x: number};
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        for await (const v of tx.scan<V>()) {
          assertType<DeepReadonly<V>>(v);
          // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
          const v2: {x: string} = v;
          use(v2);
        }

        for await (const v of tx.scan<V>().values()) {
          assertType<DeepReadonly<V>>(v);
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
      assertType<DeepReadonly<V>>(v);
      // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
      const v2: {x: string} = v;
      use(v2);
    }

    for await (const v of tx.scan<V>().values()) {
      assertType<DeepReadonly<V>>(v);
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
          assertType<DeepReadonly<V>>(v);
          // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
          v.x = [42];
          // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
          v.x[0] = 42;
        }

        for await (const v of tx.scan<V>().values()) {
          assertType<DeepReadonly<V>>(v);
          // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
          v.x = [42];
          // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
          v.x[0] = 42;
        }

        const vs = await tx.scan<V>().toArray();
        assertType<DeepReadonly<V>[]>(vs);
        // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
        vs[0].x = [42];
        // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
        vs[0].x[0] = 42;

        const vs2 = await tx.scan<V>().values().toArray();
        assertType<DeepReadonly<V>[]>(vs2);
        // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
        vs2[0].x = [42];
        // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
        vs2[0].x[0] = 42;
      },
    },
  });

  await rep.query(async tx => {
    for await (const v of tx.scan<V>()) {
      assertType<DeepReadonly<V>>(v);
      // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
      v.x = [42];
      // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
      v.x[0] = 42;
    }

    for await (const v of tx.scan<V>().values()) {
      assertType<DeepReadonly<V>>(v);
      // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
      v.x = [42];
      // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
      v.x[0] = 42;
    }

    const vs = await tx.scan<V>().toArray();
    assertType<DeepReadonly<V>[]>(vs);
    // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
    vs[0].x = [42];
    // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
    vs[0].x[0] = 42;

    const vs2 = await tx.scan<V>().values().toArray();
    assertType<DeepReadonly<V>[]>(vs2);
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
          assertType<readonly [string, {readonly x: number}]>(e);
        }

        assertType<(readonly [string, {readonly x: number}])[]>(
          await tx.scan<V>().entries().toArray(),
        );
      },
    },
  });

  await rep.query(async tx => {
    for await (const e of tx.scan<V>().entries()) {
      assertType<readonly [string, {readonly x: number}]>(e);
    }

    assertType<(readonly [string, {readonly x: number}])[]>(
      await tx.scan<V>().entries().toArray(),
    );
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
          assertType<EntryDeepV>(e);
        }

        const es = await tx.scan<V>({indexName: 'x'}).entries().toArray();
        assertType<EntryDeepV[]>(es);
      },
    },
  });

  await rep.query(async tx => {
    for await (const v of tx.scan<V>({indexName: 'x'}).entries()) {
      assertType<EntryDeepV>(v);
    }

    const es = await tx.scan<V>({indexName: 'x'}).entries().toArray();
    assertType<EntryDeepV[]>(es);
  });
});

test('Parameterized scan.entries invalid types', async () => {
  type V = {x: number};
  const rep = new Replicache({
    name: 'test-types',
    mutators: {
      mut: async (tx: WriteTransaction) => {
        for await (const e of tx.scan<V>().entries()) {
          assertType<readonly [string, {readonly x: number}]>(e);
          // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
          const v: readonly [string, {readonly x: string}] = e;
          use(v);
        }

        assertType<(readonly [string, {readonly x: number}])[]>(
          await tx.scan<V>().entries().toArray(),
        );
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
    for await (const e of tx.scan<V>().entries()) {
      assertType<readonly [string, {readonly x: number}]>(e);
      // @ts-expect-error Type 'number' is not assignable to type 'string'.ts(2322)
      const v: readonly [string, {x: string}] = e;
      use(v);
    }

    assertType<(readonly [string, {readonly x: number}])[]>(
      await tx.scan<V>().entries().toArray(),
    );
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
          assertType<DeepReadonly<V>>(v);
          // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
          v.x = [42];
          // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
          v.x[0] = 42;
        }

        const es = await tx.scan<V>().entries().toArray();
        assertType<(readonly [string, DeepReadonly<V>])[]>(es);
        // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
        es[0][1].x = [42];
        // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
        es[0][1].x[0] = 42;
      },
    },
  });

  await rep.query(async tx => {
    for await (const e of tx.scan<V>().entries()) {
      const [k, v] = e;
      use(k);
      assertType<DeepReadonly<V>>(v);
      // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
      v.x = [42];
      // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
      v.x[0] = 42;
    }

    const es = await tx.scan<V>().entries().toArray();
    assertType<(readonly [string, DeepReadonly<V>])[]>(es);
    // @ts-expect-error Cannot assign to 'x' because it is a read-only property.ts(2540)
    es[0][1].x = [42];
    // @ts-expect-error Index signature in type 'readonly number[]' only permits reading.ts(2542)
    es[0][1].x[0] = 42;
  });
});
