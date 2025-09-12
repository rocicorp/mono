import {describe, expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {deepFreeze} from '../frozen-json.ts';
import {
  assertHash,
  fakeHash,
  type Hash,
  makeNewFakeHashFunction,
} from '../hash.ts';
import type {Read, Store} from '../kv/store.ts';
import {TestMemStore} from '../kv/test-mem-store.ts';
import {
  using,
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from '../with-transactions.ts';
import {Chunk, createChunk, type Refs, toRefs} from './chunk.ts';
import {chunkDataKey, chunkMetaKey, chunkRefCountKey, headKey} from './key.ts';
import {ReadImpl, StoreImpl, WriteImpl} from './store-impl.ts';
import {ChunkNotFoundError} from './store.ts';
import {TestStore} from './test-store.ts';

describe('read', () => {
  test('has chunk', async () => {
    const t = async (hash: Hash, expectHas: boolean) => {
      const h = fakeHash('e5e');
      const kv = new TestMemStore();
      await withWrite(kv, async kvw => {
        await kvw.put(chunkDataKey(h), [0, 1]);
      });

      await withRead(kv, async kvr => {
        const r = new ReadImpl(kvr, assertHash);
        expect(await r.hasChunk(hash)).to.equal(expectHas);
      });
    };

    await t(fakeHash('e5e'), true);
    await t(fakeHash('cacaca'), false);
  });

  test('get chunk', async () => {
    const chunkHasher = makeNewFakeHashFunction();
    const t = async (
      data: ReadonlyJSONValue,
      refs: Refs,
      getSameChunk: boolean,
    ) => {
      const kv = new TestMemStore();
      const chunk = createChunk(data, refs, chunkHasher);
      await withWrite(kv, async kvw => {
        await kvw.put(chunkDataKey(chunk.hash), chunk.data);
        if (chunk.meta.length > 0) {
          await kvw.put(chunkMetaKey(chunk.hash), chunk.meta);
        }
      });

      await withRead(kv, async kvr => {
        const r = new ReadImpl(kvr, assertHash);
        let expected = undefined;
        let chunkHash: Hash;
        if (getSameChunk) {
          expected = chunk;
          chunkHash = expected.hash;
        } else {
          chunkHash = fakeHash('cacaca');
        }
        expect(await r.getChunk(chunkHash)).to.deep.equal(expected);
        if (expected) {
          expect(await r.getChunk(chunkHash)).to.deep.equal(expected);
        } else {
          expect(await r.getChunk(chunkHash)).to.be.undefined;
        }
      });
    };

    await t('Hello', toRefs([fakeHash('a001'), fakeHash('a002')]), true);
    await t(42, [], true);
    await t(null, toRefs([fakeHash('a001'), fakeHash('a002')]), false);
  });

  test('must get chunk missing chunks', async () => {
    await testChunkNotFoundError('read');
  });
});

describe('write', () => {
  test('put chunk', async () => {
    const chunkHasher = makeNewFakeHashFunction();
    const t = async (data: ReadonlyJSONValue, refs: Refs) => {
      const kv = new TestMemStore();
      await withWrite(kv, async kvw => {
        const w = new WriteImpl(kvw, chunkHasher, assertHash);
        const c = w.createChunk(deepFreeze(data), refs);
        await w.putChunk(c);

        const kd = chunkDataKey(c.hash);
        const km = chunkMetaKey(c.hash);

        // The chunk data should always be there.
        expect(await kvw.get(kd)).to.deep.equal(c.data);

        // The chunk meta should only be there if there were refs.
        if (refs.length === 0) {
          expect(await kvw.has(km)).to.be.false;
        } else {
          expect(await kvw.get(km)).to.deep.equal(c.meta);
        }
      });
    };

    await t(0, []);
    await t(42, []);
    await t(true, []);
    await t(false, []);
    await t('', []);
    await t('hello', []);
    await t([], []);
    await t([1], []);
    await t({}, []);
    await t({a: 42}, []);
  });

  async function assertRefCount(kvr: Read, hash: Hash, count: number) {
    const value = await kvr.get(chunkRefCountKey(hash));
    if (count === 0) {
      expect(value).to.be.undefined;
    } else {
      if (value === undefined) {
        throw new Error('value is undefined');
      }
      expect(value).to.equal(count);
    }
  }

  test('set head', async () => {
    const chunkHasher = makeNewFakeHashFunction();
    const t = async (kv: Store, name: string, hash: Hash | undefined) => {
      await withWriteNoImplicitCommit(kv, async kvw => {
        const w = new WriteImpl(kvw, chunkHasher, assertHash);
        await (hash === undefined ? w.removeHead(name) : w.setHead(name, hash));
        if (hash !== undefined) {
          const h = await kvw.get(headKey(name));
          expect(h).to.equal(hash);
        } else {
          expect(await kvw.get(headKey(name))).to.be.undefined;
        }
        await w.commit();
      });
    };

    const kv = new TestMemStore();

    const h0 = fakeHash('0');
    await t(kv, '', h0);
    await withRead(kv, async kvr => {
      await assertRefCount(kvr, h0, 1);
    });

    const h1 = fakeHash('1');
    await t(kv, '', h1);
    await withRead(kv, async kvr => {
      await assertRefCount(kvr, h1, 1);
      await assertRefCount(kvr, h0, 0);
    });

    await t(kv, 'n1', h0);
    await withRead(kv, async kvr => {
      await assertRefCount(kvr, h0, 1);
    });

    await t(kv, 'n1', h1);
    await withRead(kv, async kvr => {
      await assertRefCount(kvr, h1, 2);
      await assertRefCount(kvr, h0, 0);
    });

    await t(kv, 'n1', h1);
    await withRead(kv, async kvr => {
      await assertRefCount(kvr, h1, 2);
      await assertRefCount(kvr, h0, 0);
    });

    await t(kv, 'n1', undefined);
    await withRead(kv, async kvr => {
      await assertRefCount(kvr, h1, 1);
      await assertRefCount(kvr, h0, 0);
    });

    await t(kv, '', undefined);
    await withRead(kv, async kvr => {
      await assertRefCount(kvr, h1, 0);
      await assertRefCount(kvr, h0, 0);
    });
  });

  test('ref count invalid', async () => {
    const chunkHasher = makeNewFakeHashFunction();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const t = async (v: any, expectError?: string) => {
      const kv = new TestMemStore();
      const h = fakeHash('face1');
      await withWrite(kv, async kvw => {
        await kvw.put(chunkRefCountKey(h), v);
      });
      await withWriteNoImplicitCommit(kv, async kvw => {
        const w = new WriteImpl(kvw, chunkHasher, assertHash);
        let err;
        try {
          await w.setHead('fakehead', h);
          await w.commit();
        } catch (e) {
          err = e;
        }
        if (expectError) {
          expect(err)
            .to.be.instanceof(Error)
            .with.property('message', expectError);
        } else {
          expect(err, 'No error expected').to.be.undefined;
        }
      });
    };

    await t(0);
    await t(1);
    await t(42);
    await t(0xffff);
    await t(-1, 'Invalid ref count -1. We expect the value to be a Uint16');
    await t(-1, 'Invalid ref count -1. We expect the value to be a Uint16');
    await t(1.5, 'Invalid ref count 1.5. We expect the value to be a Uint16');
    await t(NaN, 'Invalid ref count NaN. We expect the value to be a Uint16');
    await t(
      Infinity,
      'Invalid ref count Infinity. We expect the value to be a Uint16',
    );
    await t(
      -Infinity,
      'Invalid ref count -Infinity. We expect the value to be a Uint16',
    );
    await t(
      2 ** 16,
      'Invalid ref count 65536. We expect the value to be a Uint16',
    );
  });

  test('commit rollback', async () => {
    const chunkHasher = makeNewFakeHashFunction();
    const t = async (commit: boolean, setHead: boolean) => {
      let key: string;
      const kv = new TestMemStore();
      await withWriteNoImplicitCommit(kv, async kvw => {
        const w = new WriteImpl(kvw, chunkHasher, assertHash);
        const c = w.createChunk(deepFreeze([0, 1]), []);
        await w.putChunk(c);

        key = chunkDataKey(c.hash);

        // The changes should be present inside the tx.
        expect(await kvw.has(key)).to.be.true;

        if (commit) {
          if (setHead) {
            await w.setHead('test', c.hash);
          }
          await w.commit();
        } else {
          // implicit rollback
        }
      });

      // The data should only persist if we set the head and commit.
      await withRead(kv, async kvr => {
        expect(setHead).to.be.equal(await kvr.has(key));
      });
    };
    await t(true, false);
    await t(false, false);
    await t(true, true);
  });

  test('roundtrip', async () => {
    const chunkHasher = makeNewFakeHashFunction();
    const t = async (name: string, data: ReadonlyJSONValue, refs: Refs) => {
      const kv = new TestMemStore();
      const hash = chunkHasher();
      const c = new Chunk(hash, deepFreeze(data), refs);
      await withWrite(kv, async kvw => {
        const w = new WriteImpl(kvw, chunkHasher, assertHash);
        await w.putChunk(c);
        await w.setHead(name, c.hash);

        // Read the changes inside the tx.
        const c2 = await w.getChunk(c.hash);
        const h = await w.getHead(name);
        expect(c2).to.deep.equal(c);
        expect(c.hash).to.equal(h);
      });

      // Read the changes outside the tx.
      await withRead(kv, async kvr => {
        const r = new ReadImpl(kvr, assertHash);
        const c2 = await r.getChunk(c.hash);
        const h = await r.getHead(name);
        expect(c2).to.deep.equal(c);
        expect(c.hash).to.equal(h);
      });
    };

    await t('', 0, []);
    await t('n1', 1, [fakeHash('a001')]);
    await t('n2', 42, toRefs([fakeHash('a001'), fakeHash('a002')]));

    await t('', true, []);
    await t('', false, []);
    await t('', [], []);
    await t('', {}, []);
    await t('', null, []);
    await t('', [0], []);
    await t('', {a: true}, []);
  });

  test('that we check if the hash is good when committing', async () => {
    const chunkHasher = makeNewFakeHashFunction();

    const t = async (
      chunkHasher: () => Hash,
      assertValidHash: (h: Hash) => void,
    ) => {
      const store = new StoreImpl(
        new TestMemStore(),
        chunkHasher,
        assertValidHash,
      );

      const data = deepFreeze([true, 42]);

      await withWrite(store, async dagWrite => {
        const c = dagWrite.createChunk(data, []);
        await dagWrite.putChunk(c);
        await dagWrite.setHead('test', c.hash);
      });

      await withRead(store, async dagRead => {
        const h = await dagRead.getHead('test');
        assert(h);
        const c = await dagRead.getChunk(h);
        assert(c);
        expect(c.hash).to.equal(h);
        expect(c.data).to.deep.equal(data);
      });
    };

    {
      let counter = 0;
      const prefix = 'testhash';
      const hasher = () =>
        (counter++).toString().padStart(32, 'testhash') as unknown as Hash;
      const testHash = (hash: Hash) => {
        assert(hash.toString().startsWith(prefix));
      };

      await t(hasher, testHash);
      await t(chunkHasher, assertHash);
    }
  });

  async function expectUndefinedForAllChunkKeys(kvRead: Read, hash: Hash) {
    expect(await kvRead.get(chunkRefCountKey(hash))).to.be.undefined;
    expect(await kvRead.get(chunkDataKey(hash))).to.be.undefined;
    expect(await kvRead.get(chunkMetaKey(hash))).to.be.undefined;
  }

  test('that we update ref counts and delete chunks (all keys) when their ref count goes to zero', async () => {
    const dagStore = new TestStore();

    //    R
    //  / |
    //  A B
    //  \ |
    //    C
    //    |
    //    D

    const d = new Chunk(fakeHash('d'), 'd', []);
    const c = new Chunk(fakeHash('c'), 'c', [d.hash]);
    const a = new Chunk(fakeHash('a'), 'a', [c.hash]);
    const b = new Chunk(fakeHash('b'), 'b', [c.hash]);
    const r = new Chunk(fakeHash('000'), 'r', toRefs([a.hash, b.hash]));
    await withWrite(dagStore, async dagWrite => {
      await Promise.all([
        dagWrite.setHead('test', r.hash),
        dagWrite.putChunk(a),
        dagWrite.putChunk(b),
        dagWrite.putChunk(c),
        dagWrite.putChunk(d),
        dagWrite.putChunk(r),
      ]);
    });

    await withRead(dagStore.kvStore, async kvRead => {
      expect(await kvRead.get(chunkRefCountKey(r.hash))).to.equal(1);
      expect(await kvRead.get(chunkRefCountKey(a.hash))).to.equal(1);
      expect(await kvRead.get(chunkRefCountKey(b.hash))).to.equal(1);
      expect(await kvRead.get(chunkRefCountKey(c.hash))).to.equal(2);
      expect(await kvRead.get(chunkRefCountKey(d.hash))).to.equal(1);
    });

    // E
    // |
    // D

    const e = new Chunk(fakeHash('e'), 'e', [d.hash]);
    await withWrite(dagStore, async dagWrite => {
      await Promise.all([
        dagWrite.setHead('test', e.hash),
        dagWrite.putChunk(e),
      ]);
    });

    await withRead(dagStore.kvStore, async kvRead => {
      await expectUndefinedForAllChunkKeys(kvRead, r.hash);
      await expectUndefinedForAllChunkKeys(kvRead, a.hash);
      await expectUndefinedForAllChunkKeys(kvRead, b.hash);
      await expectUndefinedForAllChunkKeys(kvRead, c.hash);
      expect(await kvRead.get(chunkRefCountKey(d.hash))).to.equal(1);
      expect(await kvRead.get(chunkDataKey(d.hash))).to.equal('d');
      expect(await kvRead.get(chunkRefCountKey(e.hash))).to.equal(1);
      expect(await kvRead.get(chunkDataKey(e.hash))).to.equal('e');
    });
  });

  test('must get chunk missing chunks', async () => {
    await testChunkNotFoundError('write');
  });
});

async function testChunkNotFoundError(methodName: 'read' | 'write') {
  const chunkHasher = makeNewFakeHashFunction();
  const store = new StoreImpl(new TestMemStore(), chunkHasher, assertHash);

  const data = 42;

  const h = await withWrite(store, async dagWrite => {
    const c = dagWrite.createChunk(data, []);
    await dagWrite.putChunk(c);
    await dagWrite.setHead('test', c.hash);
    return c.hash;
  });

  await using(store[methodName](), async r => {
    const chunk = await r.mustGetChunk(h);
    expect(chunk.data).to.deep.equal(data);

    let err;
    try {
      await r.mustGetChunk(fakeHash('cacaca'));
    } catch (e) {
      err = e;
    }
    expect(err)
      .to.be.instanceof(ChunkNotFoundError)
      .with.property('hash', fakeHash('cacaca'));
  });
}
