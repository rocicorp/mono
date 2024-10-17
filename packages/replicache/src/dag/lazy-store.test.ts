import {expect, test} from 'vitest';
import {deepFreeze} from '../frozen-json.js';
import {assertHash, fakeHash, makeNewFakeHashFunction} from '../hash.js';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from '../with-transactions.js';
import {asRefs, toRefs, type Chunk} from './chunk.js';
import {TestLazyStore} from './test-lazy-store.js';
import {TestStore} from './test-store.js';

const DEFAULT_VALUE_SIZE = 100;
function getSizeOfChunkForTest(chunk: Chunk): number {
  const value = chunk.data;
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const {size} = value as {size?: unknown};
    if (typeof size === 'number') {
      return size;
    }
  }
  return DEFAULT_VALUE_SIZE;
}

const DEFAULT_CACHE_SIZE_LIMIT = 200;
function createLazyStoreForTest(
  options: {
    cacheSizeLimit?: number;
  } = {},
) {
  const {cacheSizeLimit = DEFAULT_CACHE_SIZE_LIMIT} = options;
  const chunkHasher = makeNewFakeHashFunction('50ce');
  const sourceStore = new TestStore(undefined, chunkHasher, assertHash);
  return {
    sourceStore,
    chunkHasher,
    lazyStore: new TestLazyStore(
      sourceStore,
      cacheSizeLimit,
      chunkHasher,
      assertHash,
      getSizeOfChunkForTest,
    ),
  };
}

test('isMemOnlyChunkHash', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1SourceChunk = await withWrite(sourceStore, async write => {
    const chunk = write.createChunk('testValue1', []);
    await write.putChunk(chunk);
    await write.setHead('test', chunk.hash);
    return chunk;
  });
  await withRead(lazyStore, read => {
    // not true for source chunk
    expect(read.isMemOnlyChunkHash(testValue1SourceChunk.hash)).to.be.false;
  });
  const testValue2MemOnlyChunk = await withWriteNoImplicitCommit(
    lazyStore,
    async write => {
      const chunk = write.createChunk('testValue2', []);
      // not true if chunk not put
      expect(write.isMemOnlyChunkHash(chunk.hash)).to.be.false;
      await write.putChunk(chunk);
      // true inside transaction once chunk put
      expect(write.isMemOnlyChunkHash(chunk.hash)).to.be.true;
      await write.setHead('test', chunk.hash);
      // don't commit
      return chunk;
    },
  );
  await withRead(lazyStore, read => {
    // not true because not committed
    expect(read.isMemOnlyChunkHash(testValue2MemOnlyChunk.hash)).to.be.false;
  });

  const testValue3MemOnlyChunk = await withWrite(lazyStore, async write => {
    const chunk = write.createChunk('testValue3', []);
    // not true if chunk not put
    expect(write.isMemOnlyChunkHash(chunk.hash)).to.be.false;
    await write.putChunk(chunk);
    // true inside transaction once chunk put
    expect(write.isMemOnlyChunkHash(chunk.hash)).to.be.true;
    // don't retain with head

    return chunk;
  });

  await withRead(lazyStore, read => {
    // not true because not retained with head
    expect(read.isMemOnlyChunkHash(testValue3MemOnlyChunk.hash)).to.be.false;
  });

  const testValue4MemOnlyChunk = await withWrite(lazyStore, async write => {
    const chunk = write.createChunk('testValue4', []);
    // not true if chunk not put
    expect(write.isMemOnlyChunkHash(chunk.hash)).to.be.false;
    await write.putChunk(chunk);
    // true inside transaction once chunk put
    expect(write.isMemOnlyChunkHash(chunk.hash)).to.be.true;
    await write.setHead('test', chunk.hash);
    return chunk;
  });

  await withRead(lazyStore, read => {
    expect(read.isMemOnlyChunkHash(testValue4MemOnlyChunk.hash)).to.be.true;
  });

  await withWrite(lazyStore, async write => {
    await write.removeHead('test');
    // true because GC does not happen till commit.
    expect(write.isMemOnlyChunkHash(testValue4MemOnlyChunk.hash)).to.be.true;
  });

  await withRead(lazyStore, read => {
    // false because gc'd
    expect(read.isMemOnlyChunkHash(testValue4MemOnlyChunk.hash)).to.be.false;
  });
});

test('chunksPersisted', async () => {
  const {lazyStore} = createLazyStoreForTest();
  const [
    testValue1MemOnlyChunk,
    testValue2MemOnlyChunk,
    testValue3MemOnlyChunk,
  ] = await withWrite(lazyStore, async write => {
    const chunks = [];
    let refs = asRefs([]);
    for (let i = 1; i <= 3; i++) {
      const chunk = write.createChunk(`testValue${i}`, refs);
      await write.putChunk(chunk);
      refs = [chunk.hash];
      chunks.push(chunk);
    }
    await write.setHead('test-head', chunks[chunks.length - 1].hash);
    return chunks;
  });
  await withRead(lazyStore, read => {
    expect(read.isMemOnlyChunkHash(testValue1MemOnlyChunk.hash)).to.be.true;
    expect(read.isMemOnlyChunkHash(testValue2MemOnlyChunk.hash)).to.be.true;
    expect(read.isMemOnlyChunkHash(testValue3MemOnlyChunk.hash)).to.be.true;
  });

  await withWrite(lazyStore, write => {
    write.chunksPersisted([
      testValue1MemOnlyChunk.hash,
      testValue3MemOnlyChunk.hash,
    ]);
  });

  await withRead(lazyStore, read => {
    expect(read.isMemOnlyChunkHash(testValue1MemOnlyChunk.hash)).to.be.false;
    expect(read.isMemOnlyChunkHash(testValue2MemOnlyChunk.hash)).to.be.true;
    expect(read.isMemOnlyChunkHash(testValue3MemOnlyChunk.hash)).to.be.false;
  });
});

test(
  'chunks with non-memory-only hashes are loaded from source store and ' +
    'cached if reachable from a LazyStore head',
  async () => {
    const {sourceStore, lazyStore} = createLazyStoreForTest();
    const testValue1 = 'testValue1';
    const testValue1Hash = await withWrite(sourceStore, async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource', testValue1Chunk.hash);
      return testValue1Chunk.hash;
    });

    await withWrite(lazyStore, async write => {
      await write.setHead('testHeadLazy', testValue1Hash);
    });

    await withRead(lazyStore, async read => {
      expect((await read.getChunk(testValue1Hash))?.data).to.equal(testValue1);
    });
    await withWrite(sourceStore, async write => {
      await write.removeHead('testHeadSource');
    });
    await withRead(sourceStore, async read => {
      expect(await read.getChunk(testValue1Hash)).to.be.undefined;
    });
    await withRead(lazyStore, async read => {
      // value of testValue1Hash is cached
      expect((await read.getChunk(testValue1Hash))?.data).to.equal(testValue1);
    });
  },
);

test(
  'chunks with non-memory-only hashes are loaded from source store but ' +
    'not cached if not reachable from a LazyStore head',
  async () => {
    const {sourceStore, lazyStore} = createLazyStoreForTest();
    const testValue1 = 'testValue1';
    const testValue1Hash = await withWrite(sourceStore, async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource', testValue1Chunk.hash);
      return testValue1Chunk.hash;
    });
    await withRead(lazyStore, async read => {
      expect((await read.getChunk(testValue1Hash))?.data).to.equal(testValue1);
    });
    await withWrite(sourceStore, async write => {
      await write.removeHead('testHeadSource');
    });
    await withRead(sourceStore, async read => {
      expect(await read.getChunk(testValue1Hash)).to.be.undefined;
    });
    await withRead(lazyStore, async read => {
      // value of testValue1Hash is not cached
      expect(await read.getChunk(testValue1Hash)).to.be.undefined;
    });
  },
);

test('heads are *not* loaded from source store', async () => {
  const {sourceStore, chunkHasher, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1';
  const testValue1Hash = chunkHasher();
  await withRead(lazyStore, async read => {
    expect(await read.getChunk(testValue1Hash)).to.be.undefined;
  });
  await withWrite(sourceStore, async write => {
    const testValue1Chunk = write.createChunk(testValue1, []);
    await write.putChunk(testValue1Chunk);
    await write.setHead('testHeadSource', testValue1Chunk.hash);
  });
  await withRead(lazyStore, async read => {
    expect(await read.getHead('testHeadSource')).to.be.undefined;
  });
});

test('setHead stores head in memory but does not write through to source store', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  await withRead(lazyStore, async read => {
    expect(await read.getHead('testHead1')).to.be.undefined;
  });
  const fakeHash1 = fakeHash('face');
  await withWrite(lazyStore, async write => {
    await write.setHead('testHead1', fakeHash1);
  });
  await withRead(lazyStore, async read => {
    expect(await read.getHead('testHead1')).to.equal(fakeHash1);
  });
  await withRead(sourceStore, async read => {
    expect(await read.getHead('testHead1')).to.be.undefined;
  });
});

test('removeHead removes head from memory but does not write through to source store', async () => {
  const {sourceStore, chunkHasher, lazyStore} = createLazyStoreForTest();
  await withRead(lazyStore, async read => {
    expect(await read.getHead('testHead1')).to.be.undefined;
  });
  const fakeHash1 = fakeHash('face');
  await withWrite(lazyStore, async write => {
    await write.setHead('testHead1', fakeHash1);
  });
  const testValue1Hash = chunkHasher();
  await withWrite(sourceStore, async write => {
    await write.setHead('testHead1', testValue1Hash);
  });
  await withRead(lazyStore, async read => {
    expect(await read.getHead('testHead1')).to.equal(fakeHash1);
  });
  await withRead(sourceStore, async read => {
    expect(await read.getHead('testHead1')).to.equal(testValue1Hash);
  });
  await withWrite(lazyStore, async write => {
    await write.removeHead('testHead1');
  });
  await withRead(lazyStore, async read => {
    expect(await read.getHead('testHead1')).to.be.undefined;
  });
  await withRead(sourceStore, async read => {
    expect(await read.getHead('testHead1')).to.equal(testValue1Hash);
  });
});

test('putChunk with memory-only hashes updates memory but does not write through to source store', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1';
  const testValue1Chunk = await withWrite(lazyStore, async write => {
    const testValue1Chunk = write.createChunk(testValue1, []);
    // Set a head to testValue1Chunk's hash so that if it was written through to source it wouldn't
    // be gc'd
    await withWrite(sourceStore, async write => {
      await write.setHead('testHeadSource', testValue1Chunk.hash);
    });
    await write.putChunk(testValue1Chunk);
    await write.setHead('testHeadLazy', testValue1Chunk.hash);
    return testValue1Chunk;
  });
  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
  });
  await withRead(sourceStore, async read => {
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
  });
});

test('writes are visible within same write transaction but not other transactions when not committed', async () => {
  const {lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1';
  const testValue1Chunk = await withWriteNoImplicitCommit(
    lazyStore,
    async write => {
      const chunk = write.createChunk(testValue1, []);
      await write.putChunk(chunk);
      await write.setHead('testHeadLazy', chunk.hash);
      // visible within this write transaction
      expect((await write.getChunk(chunk.hash))?.data).to.equal(testValue1);
      expect(await write.getHead('testHeadLazy')).to.equal(chunk.hash);
      // do not commit
      return chunk;
    },
  );
  await withRead(lazyStore, async read => {
    // was never committed, so not visible in another transaction
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
    expect(await read.getHead('testHeadLazy')).to.be.undefined;
  });
});

async function setupTestChunks(
  sourceStore: TestStore,
  lazyStore: TestLazyStore,
) {
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3';
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} = await withWrite(
    sourceStore,
    async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      return {testValue1Chunk, testValue2Chunk, testValue3Chunk};
    },
  );

  await withWrite(lazyStore, async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
  });
  return {testValue1Chunk, testValue2Chunk, testValue3Chunk};
}

test('cache evicts in lru fashion, basic test of just reads', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await setupTestChunks(sourceStore, lazyStore);

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1Chunk.data,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2Chunk.data,
    );
    // evicts testValue1Chunk
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3Chunk.data,
    );
  });

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue2Chunk.hash,
    testValue3Chunk.hash,
  ]);
});

test('cache eviction suspension, basic test of just reads', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await setupTestChunks(sourceStore, lazyStore);

  await lazyStore.withSuspendedSourceCacheEvictsAndDeletes(async () => {
    await withRead(lazyStore, async read => {
      expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
        testValue1Chunk.data,
      );
      expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
        testValue2Chunk.data,
      );
      // would evict testValue1Chunk, but doesn't because evicts suspended
      expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
        testValue3Chunk.data,
      );
    });
    expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
      testValue1Chunk.hash,
      testValue2Chunk.hash,
      testValue3Chunk.hash,
    ]);
  });

  // testValue1Chunk now evicted
  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue2Chunk.hash,
    testValue3Chunk.hash,
  ]);
});

test('source store values are reloaded if evicted from cache', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await setupTestChunks(sourceStore, lazyStore);

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1Chunk.data,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2Chunk.data,
    );
    // evicts testValue1Chunk
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3Chunk.data,
    );
  });

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue2Chunk.hash,
    testValue3Chunk.hash,
  ]);

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1Chunk.data,
    );
  });

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue1Chunk.hash,
    testValue3Chunk.hash,
  ]);
});

test('cache evicts in lru fashion, slightly more complex test with repeats of just reads', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await setupTestChunks(sourceStore, lazyStore);

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1Chunk.data,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2Chunk.data,
    );
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1Chunk.data,
    );
    // evicts testValue2Chunk
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3Chunk.data,
    );
  });

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue1Chunk.hash,
    testValue3Chunk.hash,
  ]);
});

test('cache evicts in lru fashion, basic test of evict on write', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await setupTestChunks(sourceStore, lazyStore);

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1Chunk.data,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2Chunk.data,
    );
  });

  await withWrite(lazyStore, async write => {
    // Evicts testValue1Chunk
    await write.getChunk(testValue3Chunk.hash);
  });

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue2Chunk.hash,
    testValue3Chunk.hash,
  ]);
});

test('cache eviction suspension, basic test of evict on write', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await setupTestChunks(sourceStore, lazyStore);

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1Chunk.data,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2Chunk.data,
    );
  });

  await lazyStore.withSuspendedSourceCacheEvictsAndDeletes(async () => {
    await withWrite(lazyStore, async write => {
      // would evict testValue1Chunk, but doesn't because evicts are suspended
      await write.getChunk(testValue3Chunk.hash);
    });
    expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
      testValue1Chunk.hash,
      testValue2Chunk.hash,
      testValue3Chunk.hash,
    ]);
  });

  // now testValue1Chunk is evicted
  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue2Chunk.hash,
    testValue3Chunk.hash,
  ]);
});

test('cache will evict multiple chunks to make room for newly read chunk', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 300,
  });
  const testValue1 = 'testValue1';
  const testValue2 = 'testValue2';
  const testValue3 = 'testValue3';
  const testValue4 = deepFreeze({name: 'testValue4', size: 200});
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk, testValue4Chunk} =
    await withWrite(sourceStore, async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      const testValue4Chunk = write.createChunk(testValue4, []);
      await write.putChunk(testValue4Chunk);
      await write.setHead('testHeadSource4', testValue4Chunk.hash);
      return {
        testValue1Chunk,
        testValue2Chunk,
        testValue3Chunk,
        testValue4Chunk,
      };
    });

  await withWrite(lazyStore, async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.setHead('testHeadLazy4', testValue4Chunk.hash);
  });

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
    // evicts testValue1Chunk and testValue2Chunk as its size is 200
    expect((await read.getChunk(testValue4Chunk.hash))?.data).to.deep.equal(
      testValue4,
    );
  });

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue3Chunk.hash,
    testValue4Chunk.hash,
  ]);
});

test('cache will evict multiple chunks to make room for newly cached chunk on Write.commit', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 300,
  });
  const testValue1 = 'testValue1';
  const testValue2 = 'testValue2';
  const testValue3 = 'testValue3';
  const testValue4 = deepFreeze({name: 'testValue4', size: 200});
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk, testValue4Chunk} =
    await withWrite(sourceStore, async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      const testValue4Chunk = write.createChunk(testValue4, []);
      await write.putChunk(testValue4Chunk);
      await write.setHead('testHeadSource4', testValue4Chunk.hash);
      return {
        testValue1Chunk,
        testValue2Chunk,
        testValue3Chunk,
        testValue4Chunk,
      };
    });

  await withWrite(lazyStore, async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.setHead('testHeadLazy4', testValue4Chunk.hash);
  });

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });

  await withWrite(lazyStore, async write => {
    // evicts testValue1Chunk and testValue2Chunk as its size is 200
    await write.getChunk(testValue4Chunk.hash);
  });

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue3Chunk.hash,
    testValue4Chunk.hash,
  ]);
});

test('cache will evict all cached values to make room for new chunk', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 300,
  });
  const testValue1 = 'testValue1';
  const testValue2 = 'testValue2';
  const testValue3 = 'testValue3';
  const testValue4 = deepFreeze({name: 'testValue4', size: 250});
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk, testValue4Chunk} =
    await withWrite(sourceStore, async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      const testValue4Chunk = write.createChunk(testValue4, []);
      await write.putChunk(testValue4Chunk);
      await write.setHead('testHeadSource4', testValue4Chunk.hash);
      return {
        testValue1Chunk,
        testValue2Chunk,
        testValue3Chunk,
        testValue4Chunk,
      };
    });

  await withWrite(lazyStore, async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.setHead('testHeadLazy4', testValue4Chunk.hash);
  });

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
    // evicts testValue1Chunk, testValue2Chunk, and testValue3Chunk as its size is 250
    expect((await read.getChunk(testValue4Chunk.hash))?.data).to.deep.equal(
      testValue4,
    );
  });

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue4Chunk.hash,
  ]);
});

test(
  'cache does not cache read chunks with size greater than cacheSizeLimit, ' +
    'and does not evict other chunks to try to make room',
  async () => {
    const {sourceStore, lazyStore} = createLazyStoreForTest({
      cacheSizeLimit: 300,
    });
    const testValue1 = 'testValue1';
    const testValue2 = 'testValue2';
    const testValue3 = 'testValue3';
    const testValue4 = deepFreeze({name: 'testValue4', size: 400});
    const {testValue1Chunk, testValue2Chunk, testValue3Chunk, testValue4Chunk} =
      await withWrite(sourceStore, async write => {
        const testValue1Chunk = write.createChunk(testValue1, []);
        await write.putChunk(testValue1Chunk);
        await write.setHead('testHeadSource1', testValue1Chunk.hash);
        const testValue2Chunk = write.createChunk(testValue2, []);
        await write.putChunk(testValue2Chunk);
        await write.setHead('testHeadSource2', testValue2Chunk.hash);
        const testValue3Chunk = write.createChunk(testValue3, []);
        await write.putChunk(testValue3Chunk);
        await write.setHead('testHeadSource3', testValue3Chunk.hash);
        const testValue4Chunk = write.createChunk(testValue4, []);
        await write.putChunk(testValue4Chunk);
        await write.setHead('testHeadSource4', testValue4Chunk.hash);
        return {
          testValue1Chunk,
          testValue2Chunk,
          testValue3Chunk,
          testValue4Chunk,
        };
      });

    await withWrite(lazyStore, async write => {
      await write.setHead('testHeadLazy1', testValue1Chunk.hash);
      await write.setHead('testHeadLazy2', testValue2Chunk.hash);
      await write.setHead('testHeadLazy3', testValue3Chunk.hash);
      await write.setHead('testHeadLazy4', testValue4Chunk.hash);
    });

    await withRead(lazyStore, async read => {
      expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
        testValue1,
      );
      expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
        testValue2,
      );
      expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
        testValue3,
      );
      // is not cached because its size exceeds cache size limit
      // other chunks are not evicted
      expect((await read.getChunk(testValue4Chunk.hash))?.data).to.deep.equal(
        testValue4,
      );
    });

    expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
      testValue1Chunk.hash,
      testValue2Chunk.hash,
      testValue3Chunk.hash,
    ]);
  },
);

test(
  'on write commit cache does not cache chunks with size greater than ' +
    'cacheSizeLimit, and does not evict other chunks to try to make room',
  async () => {
    const {sourceStore, lazyStore} = createLazyStoreForTest({
      cacheSizeLimit: 200,
    });
    const testValue1 = 'testValue1';
    const testValue2 = 'testValue2';
    const testValue3 = 'testValue3';
    const testValue4 = deepFreeze({name: 'testValue4', size: 400});
    const testValue5 = deepFreeze({name: 'testValue5', size: 400});
    const {
      testValue1Chunk,
      testValue2Chunk,
      testValue3Chunk,
      testValue4Chunk,
      testValue5Chunk,
    } = await withWrite(sourceStore, async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      const testValue4Chunk = write.createChunk(testValue4, []);
      await write.putChunk(testValue4Chunk);
      await write.setHead('testHeadSource4', testValue4Chunk.hash);
      const testValue5Chunk = write.createChunk(testValue5, []);
      await write.putChunk(testValue5Chunk);
      await write.setHead('testHeadSource5', testValue5Chunk.hash);
      return {
        testValue1Chunk,
        testValue2Chunk,
        testValue3Chunk,
        testValue4Chunk,
        testValue5Chunk,
      };
    });

    await withWrite(lazyStore, async write => {
      await write.setHead('testHeadLazy1', testValue1Chunk.hash);
      await write.setHead('testHeadLazy2', testValue2Chunk.hash);
      await write.setHead('testHeadLazy3', testValue3Chunk.hash);
      await write.setHead('testHeadLazy4', testValue4Chunk.hash);
      await write.setHead('testHeadLazy5', testValue5Chunk.hash);
    });

    await withRead(lazyStore, async read => {
      expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
        testValue1,
      );
      expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
        testValue2,
      );
    });

    await withWrite(lazyStore, async write => {
      // evicts testValue1Chunk
      await write.getChunk(testValue3Chunk.hash);
      // testValue4Chunk and testValue5Chunk are not cached because each of
      // their sizes exceeds cache size limit. Other chunks are not evicted.
      await write.getChunk(testValue4Chunk.hash);
      await write.getChunk(testValue5Chunk.hash);
    });

    expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
      testValue2Chunk.hash,
      testValue3Chunk.hash,
    ]);
  },
);

test('cache eviction does not change ref counts or remove refs', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 300,
  });
  const testValue1 = 'testValue1';
  const testValue2 = 'testValue2';
  const testValue3 = deepFreeze({name: 'testValue3', size: 200});
  const testValue4 = 'testValue4';
  //    4
  //  / |
  //  3 2
  //    |
  //    1
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk, testValue4Chunk} =
    await withWrite(sourceStore, async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      const testValue2Chunk = write.createChunk(testValue2, [
        testValue1Chunk.hash,
      ]);
      await write.putChunk(testValue2Chunk);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      const testValue4Chunk = write.createChunk(
        testValue4,
        toRefs([testValue2Chunk.hash, testValue3Chunk.hash]),
      );
      await write.putChunk(testValue4Chunk);
      await write.setHead('testHeadSource', testValue4Chunk.hash);
      return {
        testValue1Chunk,
        testValue2Chunk,
        testValue3Chunk,
        testValue4Chunk,
      };
    });

  await withWrite(lazyStore, async write => {
    await write.setHead('testHeadLazy', testValue4Chunk.hash);
  });

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(testValue4Chunk.hash))?.data).to.equal(
      testValue4,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    // Current LRU order 4, 2, 1
    // Update LRU order to 2, 4, 1
    // 2, 1, 4
    expect((await read.getChunk(testValue4Chunk.hash))?.data).to.equal(
      testValue4,
    );
    // 2, 4, 1
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
  });

  expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
    [testValue1Chunk.hash]: 1,
    [testValue2Chunk.hash]: 1,
    [testValue3Chunk.hash]: 1,
    [testValue4Chunk.hash]: 1,
  });
  expect(lazyStore.getRefsSnapshot()).to.deep.equal({
    [testValue1Chunk.hash]: [],
    [testValue2Chunk.hash]: [testValue1Chunk.hash],
    [testValue4Chunk.hash]: [testValue2Chunk.hash, testValue3Chunk.hash],
  });

  await withRead(lazyStore, async read => {
    // To make room for 3 (of size 200), 2 chunks of size 100 need to be
    // removed from cache.  2 and 4 are least recently used, and so are evicted
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });

  // Ref counts are unchanged, refs were not removed
  expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
    [testValue1Chunk.hash]: 1,
    [testValue2Chunk.hash]: 1,
    [testValue3Chunk.hash]: 1,
    [testValue4Chunk.hash]: 1,
  });
  expect(lazyStore.getRefsSnapshot()).to.deep.equal({
    [testValue1Chunk.hash]: [],
    [testValue2Chunk.hash]: [testValue1Chunk.hash],
    [testValue3Chunk.hash]: [],
    [testValue4Chunk.hash]: [testValue2Chunk.hash, testValue3Chunk.hash],
  });

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue1Chunk.hash,
    testValue3Chunk.hash,
  ]);

  // Delete chunks from lazy store
  await withWrite(lazyStore, async write => {
    await write.removeHead('testHeadLazy');
  });

  // Refs and ref counts of delete chunks are deleted
  expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({});
  expect(lazyStore.getRefsSnapshot()).to.deep.equal({});
  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([]);
});

test('memory-only chunks are not evicted when cache size is exceeded', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3';
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} = await withWrite(
    sourceStore,
    async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      return {testValue1Chunk, testValue2Chunk, testValue3Chunk};
    },
  );
  const tempValue1 = 'tempValue1',
    tempValue2 = 'tempValue2';
  const {tempValue1Chunk, tempValue2Chunk} = await withWrite(
    lazyStore,
    async write => {
      const tempValue1Chunk = write.createChunk(tempValue1, []);
      await write.putChunk(tempValue1Chunk);
      await write.setHead('tempHeadLazy1', tempValue1Chunk.hash);
      const tempValue2Chunk = write.createChunk(tempValue2, []);
      await write.putChunk(tempValue2Chunk);
      await write.setHead('tempHeadLazy2', tempValue2Chunk.hash);

      await write.setHead('testHeadLazy1', testValue1Chunk.hash);
      await write.setHead('testHeadLazy2', testValue2Chunk.hash);
      await write.setHead('testHeadLazy3', testValue3Chunk.hash);

      return {tempValue1Chunk, tempValue2Chunk};
    },
  );

  await withRead(lazyStore, async read => {
    expect((await read.getChunk(tempValue1Chunk.hash))?.data).to.equal(
      tempValue1,
    );
    expect((await read.getChunk(tempValue2Chunk.hash))?.data).to.equal(
      tempValue2,
    );
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    // over cache size limit, should evict testValue1Chunk, but not any
    // memory-only chunks
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
    testValue2Chunk.hash,
    testValue3Chunk.hash,
  ]);

  await withRead(lazyStore, async read => {
    // memory-only chunks were not evicted
    expect((await read.getChunk(tempValue1Chunk.hash))?.data).to.equal(
      tempValue1,
    );
    expect((await read.getChunk(tempValue2Chunk.hash))?.data).to.equal(
      tempValue2,
    );
  });
});

test(
  '[all memory-only chunks] chunk ref counts are updated on commit and ' +
    'chunks (and their refs) are deleted when their ref count goes to zero',
  async () => {
    const {lazyStore} = createLazyStoreForTest();

    //    R
    //  / |
    //  A B
    //  \ |
    //    C
    //    |
    //    D
    const {r, a, b, c, d} = await withWrite(lazyStore, async write => {
      const d = write.createChunk('d', []);
      const c = write.createChunk('c', [d.hash]);
      const b = write.createChunk('b', [c.hash]);
      const a = write.createChunk('a', [c.hash]);
      const r = write.createChunk('r', toRefs([b.hash, a.hash]));
      await write.putChunk(r);
      await write.putChunk(a);
      await write.putChunk(b);
      await write.putChunk(c);
      await write.putChunk(d);
      await write.setHead('test', r.hash);
      return {r, a, b, c, d};
    });

    expect(lazyStore.getRefsSnapshot()).to.deep.equal({
      [r.hash]: [b.hash, a.hash],
      [a.hash]: [c.hash],
      [b.hash]: [c.hash],
      [c.hash]: [d.hash],
      [d.hash]: [],
    });
    expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
      [r.hash]: 1,
      [a.hash]: 1,
      [b.hash]: 1,
      [c.hash]: 2,
      [d.hash]: 1,
    });

    // E
    // |
    // D
    const e = await withWrite(lazyStore, async write => {
      const e = write.createChunk('e', [d.hash]);
      await write.putChunk(e);
      await write.setHead('test', e.hash);
      return e;
    });

    expect(lazyStore.getRefsSnapshot()).to.deep.equal({
      [e.hash]: [d.hash],
      [d.hash]: [],
    });
    expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
      [d.hash]: 1,
      [e.hash]: 1,
    });

    await withRead(lazyStore, async read => {
      expect(await read.getChunk(r.hash)).to.be.undefined;
      expect(await read.getChunk(a.hash)).to.be.undefined;
      expect(await read.getChunk(b.hash)).to.be.undefined;
      expect(await read.getChunk(c.hash)).to.be.undefined;
      expect(await read.getChunk(d.hash)).to.deep.equal(d);
      expect(await read.getChunk(e.hash)).to.deep.equal(e);
    });
  },
);

test(
  '[all cached chunks] chunk ref counts are updated on commit and chunks ' +
    '(and their refs) are deleted when their ref count goes to zero',
  async () => {
    // Make cache size large enough that eviction does not occur
    // during test
    const {sourceStore, lazyStore} = createLazyStoreForTest({
      cacheSizeLimit: 1000,
    });

    //    R
    //  / |
    //  A B
    //  \ |
    //    C
    //    |
    //    D

    const {r, a, b, c, d} = await withWrite(sourceStore, async write => {
      const d = write.createChunk('d', []);
      const c = write.createChunk('c', [d.hash]);
      const b = write.createChunk('b', [c.hash]);
      const a = write.createChunk('a', [c.hash]);
      const r = write.createChunk('r', toRefs([b.hash, a.hash]));
      await write.putChunk(r);
      await write.putChunk(a);
      await write.putChunk(b);
      await write.putChunk(c);
      await write.putChunk(d);
      await write.setHead('testSource', r.hash);
      return {r, a, b, c, d};
    });

    await withWrite(lazyStore, async write => {
      await write.getChunk(r.hash);
      await write.getChunk(a.hash);
      await write.getChunk(b.hash);
      await write.getChunk(c.hash);
      await write.getChunk(d.hash);
      await write.setHead('testLazy', r.hash);
    });

    expect(lazyStore.getRefsSnapshot()).to.deep.equal({
      [r.hash]: [b.hash, a.hash],
      [a.hash]: [c.hash],
      [b.hash]: [c.hash],
      [c.hash]: [d.hash],
      [d.hash]: [],
    });
    expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
      [r.hash]: 1,
      [a.hash]: 1,
      [b.hash]: 1,
      [c.hash]: 2,
      [d.hash]: 1,
    });

    // E
    // |
    // D

    const e = await withWrite(sourceStore, async write => {
      const e = write.createChunk('e', [d.hash]);
      await write.putChunk(e);
      await write.setHead('testSource', e.hash);
      return e;
    });

    await withWrite(lazyStore, async write => {
      await write.getChunk(e.hash);
      await write.setHead('testLazy', e.hash);
      return e;
    });

    expect(lazyStore.getRefsSnapshot()).to.deep.equal({
      [e.hash]: [d.hash],
      [d.hash]: [],
    });
    expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
      [d.hash]: 1,
      [e.hash]: 1,
    });

    await withRead(lazyStore, async read => {
      expect(await read.getChunk(r.hash)).to.be.undefined;
      expect(await read.getChunk(a.hash)).to.be.undefined;
      expect(await read.getChunk(b.hash)).to.be.undefined;
      expect(await read.getChunk(c.hash)).to.be.undefined;
      expect(await read.getChunk(d.hash)).to.deep.equal(d);
      expect(await read.getChunk(e.hash)).to.deep.equal(e);
    });
  },
);

test(
  '[mix of memory-only and cached chunks] chunk ref counts are updated on ' +
    'commit and chunks (and their refs) are deleted when their ref count goes to zero',
  async () => {
    // Make cache size large enough that eviction does not occur
    // during test
    const {sourceStore, lazyStore} = createLazyStoreForTest({
      cacheSizeLimit: 1000,
    });

    //    R
    //  / |
    //  A B
    //  \ |
    //    C
    //    |
    //    D

    const {r, a, b, c, d} = await withWrite(sourceStore, async write => {
      const d = write.createChunk('d', []);
      const c = write.createChunk('c', [d.hash]);
      const b = write.createChunk('b', [c.hash]);
      const a = write.createChunk('a', [c.hash]);
      const r = write.createChunk('r', toRefs([b.hash, a.hash]));
      await write.putChunk(r);
      await write.putChunk(a);
      await write.putChunk(b);
      await write.putChunk(c);
      await write.putChunk(d);
      await write.setHead('testSource', r.hash);
      return {r, a, b, c, d};
    });

    await withWrite(lazyStore, async write => {
      await write.getChunk(r.hash);
      await write.getChunk(a.hash);
      await write.getChunk(b.hash);
      await write.getChunk(c.hash);
      await write.getChunk(d.hash);
      await write.setHead('testLazy', r.hash);
    });

    expect(lazyStore.getRefsSnapshot()).to.deep.equal({
      [r.hash]: [b.hash, a.hash],
      [a.hash]: [c.hash],
      [b.hash]: [c.hash],
      [c.hash]: [d.hash],
      [d.hash]: [],
    });
    expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
      [r.hash]: 1,
      [a.hash]: 1,
      [b.hash]: 1,
      [c.hash]: 2,
      [d.hash]: 1,
    });

    // tempE
    // |
    // D

    const tempE = await withWrite(lazyStore, async write => {
      const tempE = write.createChunk('e', [d.hash]);
      await write.putChunk(tempE);
      await write.setHead('testLazy', tempE.hash);
      return tempE;
    });

    expect(lazyStore.getRefsSnapshot()).to.deep.equal({
      [tempE.hash]: [d.hash],
      [d.hash]: [],
    });
    expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
      [d.hash]: 1,
      [tempE.hash]: 1,
    });

    // E
    // |
    // D
    const e = await withWrite(sourceStore, async write => {
      const e = write.createChunk('e', [d.hash]);
      await write.putChunk(e);
      await write.setHead('testSource', e.hash);
      return e;
    });

    await withWrite(lazyStore, async write => {
      await write.getChunk(e.hash);
      await write.setHead('testLazy', e.hash);
      return e;
    });

    expect(lazyStore.getRefsSnapshot()).to.deep.equal({
      [e.hash]: [d.hash],
      [d.hash]: [],
    });
    expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
      [d.hash]: 1,
      [e.hash]: 1,
    });

    await withRead(lazyStore, async read => {
      expect(await read.getChunk(r.hash)).to.be.undefined;
      expect(await read.getChunk(a.hash)).to.be.undefined;
      expect(await read.getChunk(b.hash)).to.be.undefined;
      expect(await read.getChunk(c.hash)).to.be.undefined;
      expect(await read.getChunk(tempE.hash)).to.be.undefined;
      expect(await read.getChunk(d.hash)).to.deep.equal(d);
      expect(await read.getChunk(e.hash)).to.deep.equal(e);
    });
  },
);

test(
  '[mix of memory-only and cached chunks] when source cache chunk deletes ' +
    'are suspended chunk ref counts are updated on commit and memonly chunks ' +
    '(and their refs) are deleted when their ref count goes to zero, but ' +
    'cached chunks (and their refs) are not deleted until un-suspended',
  async () => {
    // Make cache size large enough that eviction does not occur
    // during test
    const {sourceStore, lazyStore} = createLazyStoreForTest({
      cacheSizeLimit: 1000,
    });

    //    R
    //  / |
    //  A B
    //  \ |
    //    C
    //    |
    //    D

    const {r, a, b, c, d} = await withWrite(sourceStore, async write => {
      const d = write.createChunk('d', []);
      const c = write.createChunk('c', [d.hash]);
      const b = write.createChunk('b', [c.hash]);
      const a = write.createChunk('a', [c.hash]);
      const r = write.createChunk('r', toRefs([b.hash, a.hash]));
      await write.putChunk(r);
      await write.putChunk(a);
      await write.putChunk(b);
      await write.putChunk(c);
      await write.putChunk(d);
      await write.setHead('testSource', r.hash);
      return {r, a, b, c, d};
    });

    const tempR1 = await withWrite(lazyStore, async write => {
      await write.getChunk(r.hash);
      await write.getChunk(a.hash);
      await write.getChunk(b.hash);
      await write.getChunk(c.hash);
      await write.getChunk(d.hash);
      const tempR1 = await write.createChunk('tempR1', [r.hash]);
      await write.putChunk(tempR1);
      await write.setHead('testLazy', tempR1.hash);
      return tempR1;
    });

    expect(lazyStore.getRefsSnapshot()).to.deep.equal({
      [tempR1.hash]: [r.hash],
      [r.hash]: [b.hash, a.hash],
      [a.hash]: [c.hash],
      [b.hash]: [c.hash],
      [c.hash]: [d.hash],
      [d.hash]: [],
    });
    expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
      [tempR1.hash]: 1,
      [r.hash]: 1,
      [a.hash]: 1,
      [b.hash]: 1,
      [c.hash]: 2,
      [d.hash]: 1,
    });
    await withRead(lazyStore, async read => {
      expect((await read.getChunk(tempR1.hash))?.data).to.equal(tempR1.data);
    });
    expect(await lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
      r.hash,
      a.hash,
      b.hash,
      c.hash,
      d.hash,
    ]);

    const tempR2 = await lazyStore.withSuspendedSourceCacheEvictsAndDeletes(
      async () => {
        // tempR2
        // |
        // D
        const tempR2 = await withWrite(lazyStore, async write => {
          const tempR2 = write.createChunk('tempR2', [d.hash]);
          await write.putChunk(tempR2);
          await write.setHead('testLazy', tempR2.hash);
          return tempR2;
        });

        expect(lazyStore.getRefsSnapshot()).to.deep.equal({
          [tempR2.hash]: [d.hash],
          [r.hash]: [b.hash, a.hash],
          [a.hash]: [c.hash],
          [b.hash]: [c.hash],
          [c.hash]: [d.hash],
          [d.hash]: [],
        });
        expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
          [tempR2.hash]: 1,
          [r.hash]: 0,
          [a.hash]: 0,
          [b.hash]: 0,
          [c.hash]: 0,
          [d.hash]: 1,
        });
        await withRead(lazyStore, async read => {
          expect(await read.getChunk(tempR1.hash)).to.be.undefined;
          expect((await read.getChunk(tempR2.hash))?.data).to.equal(
            tempR2.data,
          );
        });
        expect(await lazyStore.getCachedSourceChunksSnapshot()).to.deep.members(
          [r.hash, a.hash, b.hash, c.hash, d.hash],
        );

        // C
        // |
        // D
        await withWrite(lazyStore, async write => {
          await write.setHead('testLazy', c.hash);
        });

        expect(lazyStore.getRefsSnapshot()).to.deep.equal({
          [r.hash]: [b.hash, a.hash],
          [a.hash]: [c.hash],
          [b.hash]: [c.hash],
          [c.hash]: [d.hash],
          [d.hash]: [],
        });
        expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
          [r.hash]: 0,
          [a.hash]: 0,
          [b.hash]: 0,
          [c.hash]: 1,
          [d.hash]: 1,
        });
        await withRead(lazyStore, async read => {
          expect(await read.getChunk(tempR1.hash)).to.be.undefined;
          expect(await read.getChunk(tempR2.hash)).to.be.undefined;
        });
        expect(await lazyStore.getCachedSourceChunksSnapshot()).to.deep.members(
          [r.hash, a.hash, b.hash, c.hash, d.hash],
        );
        return tempR2;
      },
    );

    expect(lazyStore.getRefsSnapshot()).to.deep.equal({
      [c.hash]: [d.hash],
      [d.hash]: [],
    });
    expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
      [c.hash]: 1,
      [d.hash]: 1,
    });
    await withRead(lazyStore, async read => {
      expect(await read.getChunk(tempR1.hash)).to.be.undefined;
      expect(await read.getChunk(tempR2.hash)).to.be.undefined;
    });
    expect(await lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([
      c.hash,
      d.hash,
    ]);
  },
);

test('[chunk cached via get] the refs of chunks being cached for the first time are counted on commit, even if they were already reachable.', async () => {
  // Make cache size large enough that eviction does not occur
  // during test
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 1000,
  });
  await testChunksCacheForFirstTimeRefsAreCounted(
    sourceStore,
    lazyStore,
    'get',
  );
});

test('[chunk cached via put] the refs of chunks being cached for the first time are counted on commit, even if they were already reachable.', async () => {
  // Make cache size large enough that eviction does not occur
  // during test
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 1000,
  });
  await testChunksCacheForFirstTimeRefsAreCounted(
    sourceStore,
    lazyStore,
    'put',
  );
});

async function testChunksCacheForFirstTimeRefsAreCounted(
  sourceStore: TestStore,
  lazyStore: TestLazyStore,
  cacheMethod: 'get' | 'put',
  deleteAllFromLazy = true,
): Promise<{a: Chunk; b: Chunk; c: Chunk}> {
  //  headSource
  //  |
  //  B
  //  |
  //  A
  const {a, b} = await withWrite(sourceStore, async write => {
    const a = write.createChunk('a', []);
    const b = write.createChunk('b', [a.hash]);
    await write.putChunk(a);
    await write.putChunk(b);
    await write.setHead('headSource', b.hash);
    return {a, b};
  });

  //  headLazy
  //  |
  //  B
  //  |
  //  A
  await withWrite(lazyStore, async write => {
    await write.setHead('headLazy', b.hash);
  });

  expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
    [b.hash]: 1,
  });

  //  headLazy
  //  |
  //  C
  //  |\
  //  B |
  //  |/
  //  A
  const c = await withWrite(lazyStore, async write => {
    switch (cacheMethod) {
      case 'get':
        await write.getChunk(b.hash);
        break;
      case 'put':
        await write.putChunk(b);
        break;
    }
    const c = write.createChunk('c', toRefs([b.hash, a.hash]));
    await write.putChunk(c);
    await write.setHead('headLazy', c.hash);
    return c;
  });

  // B was already reachable, its ref to A is still counted since it
  // was not never-previously cached and so the ref was lazily discovered
  // during this write.
  expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
    [c.hash]: 1,
    [b.hash]: 1,
    [a.hash]: 2,
  });

  if (deleteAllFromLazy) {
    // delete headLazy
    await withWrite(lazyStore, async write => {
      await write.removeHead('headLazy');
    });
    // If B's ref to A was not counted despite being previously
    // reachable, A would now have a negative refCount (as its refCount
    // would be 1, and then -1 for C's ref to it and -1 for B's ref to it).
    // Assert that instead everything has a refCount of zero (no entry).
    expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({});
    expect(lazyStore.getRefsSnapshot()).to.deep.equal({});
    expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([]);
  }
  return {a, b, c};
}

test('the refs of chunks being cached for a *second* time are not counted on commit', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 100, // cache only fits one
  });

  const {a, b, c} = await testChunksCacheForFirstTimeRefsAreCounted(
    sourceStore,
    lazyStore,
    'put',
    false,
  );

  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([b.hash]);

  await withRead(lazyStore, async read => {
    // B is evicted
    await read.getChunk(a.hash);
  });
  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([a.hash]);

  expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
    [c.hash]: 1,
    [b.hash]: 1,
    [a.hash]: 2,
  });

  await withWrite(lazyStore, async write => {
    // recache B, A is evicted
    await write.getChunk(b.hash);
  });
  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([b.hash]);

  // B's refs are not recounted
  expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({
    [c.hash]: 1,
    [b.hash]: 1,
    [a.hash]: 2,
  });

  // delete headLazy
  await withWrite(lazyStore, async write => {
    await write.removeHead('headLazy');
  });
  expect(lazyStore.getRefCountsSnapshot()).to.deep.equal({});
  expect(lazyStore.getRefsSnapshot()).to.deep.equal({});
  expect(lazyStore.getCachedSourceChunksSnapshot()).to.deep.members([]);
});
