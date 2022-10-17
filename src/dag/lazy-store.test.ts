import {expect} from '@esm-bundle/chai';
import {assertHash, fakeHash, makeNewFakeHashFunction} from '../hash';
import {LazyStore} from './lazy-store';
import {TestStore} from './test-store';

const DEFAULT_VALUE_SIZE = 100;
function getSizeOfValueForTest(value: unknown): number {
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
  const sourceStoreChunkHasher = makeNewFakeHashFunction('50ce');
  const lazyStoreChunkHasher = makeNewFakeHashFunction('feed');
  const sourceStore = new TestStore(
    undefined,
    sourceStoreChunkHasher,
    assertHash,
  );
  return {
    sourceStore,
    sourceStoreChunkHasher,
    lazyStore: new LazyStore(
      sourceStore,
      cacheSizeLimit,
      lazyStoreChunkHasher,
      assertHash,
      getSizeOfValueForTest,
    ),
  };
}

test('chunks with non-memory-only hashes are loaded from source store and cached if reachable from a LazyStore head', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1';
  const testValue1Hash = await sourceStore.withWrite(async write => {
    const testValue1Chunk = write.createChunk(testValue1, []);
    await write.putChunk(testValue1Chunk);
    await write.setHead('testHeadSource', testValue1Chunk.hash);
    await write.commit();
    return testValue1Chunk.hash;
  });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy', testValue1Hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Hash))?.data).to.equal(testValue1);
  });
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource');
    await write.commit();
  });
  await sourceStore.withRead(async read => {
    expect(await read.getChunk(testValue1Hash)).to.be.undefined;
  });
  await lazyStore.withRead(async read => {
    // value of testValue1Hash is cached
    expect((await read.getChunk(testValue1Hash))?.data).to.equal(testValue1);
  });
});

test('chunks with non-memory-only hashes are loaded from source store but not cached if not reachable from a LazyStore head', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1';
  const testValue1Hash = await sourceStore.withWrite(async write => {
    const testValue1Chunk = write.createChunk(testValue1, []);
    await write.putChunk(testValue1Chunk);
    await write.setHead('testHeadSource', testValue1Chunk.hash);
    await write.commit();
    return testValue1Chunk.hash;
  });
  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Hash))?.data).to.equal(testValue1);
  });
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource');
    await write.commit();
  });
  await sourceStore.withRead(async read => {
    expect(await read.getChunk(testValue1Hash)).to.be.undefined;
  });
  await lazyStore.withRead(async read => {
    // value of testValue1Hash is not cached
    expect(await read.getChunk(testValue1Hash)).to.be.undefined;
  });
});

test('heads are *not* loaded from source store', async () => {
  const {sourceStore, sourceStoreChunkHasher, lazyStore} =
    createLazyStoreForTest();
  const testValue1 = 'testValue1';
  const testValue1Hash = sourceStoreChunkHasher();
  await lazyStore.withRead(async read => {
    expect(await read.getChunk(testValue1Hash)).to.be.undefined;
  });
  await sourceStore.withWrite(async write => {
    const testValue1Chunk = write.createChunk(testValue1, []);
    await write.putChunk(testValue1Chunk);
    await write.setHead('testHeadSource', testValue1Chunk.hash);
    await write.commit();
  });
  await lazyStore.withRead(async read => {
    expect(await read.getHead('testHeadSource')).to.be.undefined;
  });
});

test('setHead stores head in memory but does not write through to source store', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  await lazyStore.withRead(async read => {
    expect(await read.getHead('testHead1')).to.be.undefined;
  });
  const fakeHash1 = fakeHash('face');
  await lazyStore.withWrite(async write => {
    await write.setHead('testHead1', fakeHash1);
    await write.commit();
  });
  await lazyStore.withRead(async read => {
    expect(await read.getHead('testHead1')).to.equal(fakeHash1);
  });
  await sourceStore.withRead(async read => {
    expect(await read.getHead('testHead1')).to.be.undefined;
  });
});

test('removeHead removes head from memory but does not write through to source store', async () => {
  const {sourceStore, sourceStoreChunkHasher, lazyStore} =
    createLazyStoreForTest();
  await lazyStore.withRead(async read => {
    expect(await read.getHead('testHead1')).to.be.undefined;
  });
  const fakeHash1 = fakeHash('face');
  await lazyStore.withWrite(async write => {
    await write.setHead('testHead1', fakeHash1);
    await write.commit();
  });
  const testValue1Hash = sourceStoreChunkHasher();
  await sourceStore.withWrite(async write => {
    await write.setHead('testHead1', testValue1Hash);
    await write.commit();
  });
  await lazyStore.withRead(async read => {
    expect(await read.getHead('testHead1')).to.equal(fakeHash1);
  });
  await sourceStore.withRead(async read => {
    expect(await read.getHead('testHead1')).to.equal(testValue1Hash);
  });
  await lazyStore.withWrite(async write => {
    await write.removeHead('testHead1');
    await write.commit();
  });
  await lazyStore.withRead(async read => {
    expect(await read.getHead('testHead1')).to.be.undefined;
  });
  await sourceStore.withRead(async read => {
    expect(await read.getHead('testHead1')).to.equal(testValue1Hash);
  });
});

test('putChunk with non-temp hashes updates memory but does not write through to source store', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1';
  const testValue1Chunk = await sourceStore.withWrite(async write => {
    const testValue1Chunk = write.createChunk(testValue1, []);
    await write.setHead('testHeadSource', testValue1Chunk.hash);
    await write.commit();
    return testValue1Chunk;
  });
  await lazyStore.withWrite(async write => {
    await write.putChunk(testValue1Chunk);
    await write.setHead('testHeadLazy', testValue1Chunk.hash);
    await write.commit();
  });
  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
  });
  await sourceStore.withRead(async read => {
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
  });
});

test('putChunk with temp hashes updates memory but does not write through to source store', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1';
  const testValue1Chunk = await lazyStore.withWrite(async write => {
    return write.createChunk(testValue1, []);
  });
  expect(await lazyStore.isMemOnlyChunkHash(testValue1Chunk.hash)).to.be.true;
  // Set a head to testValue1Chunk's hash so that if it was written through to source it wouldn't
  // be gc'd
  await sourceStore.withWrite(async write => {
    await write.setHead('testHeadSource', testValue1Chunk.hash);
    await write.commit();
  });
  await lazyStore.withWrite(async write => {
    await write.putChunk(testValue1Chunk);
    await write.setHead('testHeadLazy', testValue1Chunk.hash);
    await write.commit();
  });
  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
  });
  await sourceStore.withRead(async read => {
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
  });
});

test('writes are visible within same write transaction but not other transactions when not committed', async () => {
  const {lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1';
  const testValue1Chunk = await lazyStore.withWrite(async write => {
    return write.createChunk(testValue1, []);
  });
  await lazyStore.withWrite(async write => {
    await write.putChunk(testValue1Chunk);
    await write.setHead('testHeadLazy', testValue1Chunk.hash);
    // visible within this write transaction
    expect((await write.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect(await write.getHead('testHeadLazy')).to.equal(testValue1Chunk.hash);
    // do not commit
  });
  await lazyStore.withRead(async read => {
    // was never committed, so not visible in another transaction
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
    expect(await read.getHead('testHeadLazy')).to.be.undefined;
  });
});

test('cache evicts in lru fashion, basic test of just reads', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3';
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await sourceStore.withWrite(async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      await write.commit();
      return {testValue1Chunk, testValue2Chunk, testValue3Chunk};
    });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    // evicts testValue1Chunk
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });

  // gc chunks from base store
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource1');
    await write.removeHead('testHeadSource2');
    await write.removeHead('testHeadSource3');
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    // testValue1Chunk was evicted and is no longer available in base store
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });
});

test('source store values are reloaded if evicted from cache', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3';
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await sourceStore.withWrite(async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      await write.commit();
      return {testValue1Chunk, testValue2Chunk, testValue3Chunk};
    });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    // evicts testValue1Chunk
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });

  // do not gc chunks from base store, so they can be reloaded
  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
  });
});

test('cache evicts in lru fashion, slightly more complex test with repeats of just reads', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3';
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await sourceStore.withWrite(async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      await write.commit();
      return {testValue1Chunk, testValue2Chunk, testValue3Chunk};
    });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    // evicts testValue2Chunk
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });

  // gc chunks from base store
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource1');
    await write.removeHead('testHeadSource2');
    await write.removeHead('testHeadSource3');
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    // testValue2Chunk was evicted and is no longer available in base store
    expect(await read.getChunk(testValue2Chunk.hash)).to.be.undefined;
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });
});

test('cache evicts in lru fashion, basic test of evict on write', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3';
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await sourceStore.withWrite(async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      await write.commit();
      return {testValue1Chunk, testValue2Chunk, testValue3Chunk};
    });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
  });

  await lazyStore.withWrite(async write => {
    // Evicts testValue1Chunk
    await write.putChunk(testValue3Chunk);
    await write.commit();
  });

  // gc chunks from base store
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource1');
    await write.removeHead('testHeadSource2');
    await write.removeHead('testHeadSource3');
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    // testValue1Chunk was evicted and is no longer available in base store
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });
});

test('cache will evict multiple chunks to make room for newly read chunk', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 300,
  });
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3',
    testValue4 = {name: 'testValue4', size: 200};
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk, testValue4Chunk} =
    await sourceStore.withWrite(async write => {
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
      await write.commit();
      return {
        testValue1Chunk,
        testValue2Chunk,
        testValue3Chunk,
        testValue4Chunk,
      };
    });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.setHead('testHeadLazy4', testValue4Chunk.hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
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

  // gc chunks from base store
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource1');
    await write.removeHead('testHeadSource2');
    await write.removeHead('testHeadSource3');
    await write.removeHead('testHeadSource4');
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    // testValue1Chunk and testValue2Chunk were evicted and are no longer available in base store
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
    expect(await read.getChunk(testValue2Chunk.hash)).to.be.undefined;
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
    expect((await read.getChunk(testValue4Chunk.hash))?.data).to.deep.equal(
      testValue4,
    );
  });
});

test('cache will evict multiple chunks to make room for newly written chunk', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 300,
  });
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3',
    testValue4 = {name: 'testValue4', size: 200};
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk, testValue4Chunk} =
    await sourceStore.withWrite(async write => {
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
      await write.commit();
      return {
        testValue1Chunk,
        testValue2Chunk,
        testValue3Chunk,
        testValue4Chunk,
      };
    });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.setHead('testHeadLazy4', testValue4Chunk.hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
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

  await lazyStore.withWrite(async write => {
    // evicts testValue1Chunk and testValue2Chunk as its size is 200
    await write.putChunk(testValue4Chunk);
    await write.commit();
  });

  // gc chunks from base store
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource1');
    await write.removeHead('testHeadSource2');
    await write.removeHead('testHeadSource3');
    await write.removeHead('testHeadSource4');
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    // testValue1Chunk and testValue2Chunk were evicted and are no longer available in base store
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
    expect(await read.getChunk(testValue2Chunk.hash)).to.be.undefined;
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
    expect((await read.getChunk(testValue4Chunk.hash))?.data).to.deep.equal(
      testValue4,
    );
  });
});

test('cache will evict all cached values to make room for new chunk', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 300,
  });
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3',
    testValue4 = {name: 'testValue4', size: 250};
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk, testValue4Chunk} =
    await sourceStore.withWrite(async write => {
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
      await write.commit();
      return {
        testValue1Chunk,
        testValue2Chunk,
        testValue3Chunk,
        testValue4Chunk,
      };
    });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.setHead('testHeadLazy4', testValue4Chunk.hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
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

  // gc chunks from base store
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource1');
    await write.removeHead('testHeadSource2');
    await write.removeHead('testHeadSource3');
    await write.removeHead('testHeadSource4');
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    // testValue1Chunk, testValue2Chunk and testValue3Chunk were evicted and are no longer available in
    // base store
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
    expect(await read.getChunk(testValue2Chunk.hash)).to.be.undefined;
    expect(await read.getChunk(testValue2Chunk.hash)).to.be.undefined;
    expect((await read.getChunk(testValue4Chunk.hash))?.data).to.deep.equal(
      testValue4,
    );
  });
});

test('cache does not cache read chunks with size greater than cacheSizeLimit, and does not evict other chunks to try to make room', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 300,
  });
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3',
    testValue4 = {name: 'testValue4', size: 400};
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk, testValue4Chunk} =
    await sourceStore.withWrite(async write => {
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
      await write.commit();
      return {
        testValue1Chunk,
        testValue2Chunk,
        testValue3Chunk,
        testValue4Chunk,
      };
    });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.setHead('testHeadLazy4', testValue4Chunk.hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
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

  // gc chunks from base store
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource1');
    await write.removeHead('testHeadSource2');
    await write.removeHead('testHeadSource3');
    await write.removeHead('testHeadSource4');
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
    expect((await read.getChunk(testValue4Chunk.hash))?.data).to.be.undefined;
  });
});

test('cache does not cache put chunks with size greater than cacheSizeLimit, and does not evict other chunks to try to make room', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 200,
  });
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3',
    testValue4 = {name: 'testValue4', size: 400},
    testValue5 = {name: 'testValue5', size: 400};
  const {
    testValue1Chunk,
    testValue2Chunk,
    testValue3Chunk,
    testValue4Chunk,
    testValue5Chunk,
  } = await sourceStore.withWrite(async write => {
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
    await write.commit();
    return {
      testValue1Chunk,
      testValue2Chunk,
      testValue3Chunk,
      testValue4Chunk,
      testValue5Chunk,
    };
  });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy1', testValue1Chunk.hash);
    await write.setHead('testHeadLazy2', testValue2Chunk.hash);
    await write.setHead('testHeadLazy3', testValue3Chunk.hash);
    await write.setHead('testHeadLazy4', testValue4Chunk.hash);
    await write.setHead('testHeadLazy5', testValue5Chunk.hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.equal(
      testValue1,
    );
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
  });

  await lazyStore.withWrite(async write => {
    // evicts testValue1Chunk
    await write.putChunk(testValue3Chunk);
    // testValue4Chunk and testValue5Chunk are not cached because each of
    // their sizes exceeds cache size limit. Other chunks are not evicted.
    await write.putChunk(testValue4Chunk);
    await write.putChunk(testValue5Chunk);
    await write.commit();
  });

  // gc chunks from base store
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource1');
    await write.removeHead('testHeadSource2');
    await write.removeHead('testHeadSource3');
    await write.removeHead('testHeadSource4');
    await write.removeHead('testHeadSource5');
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    // testValue1Chunk was evicted and is no longer available in base store
    expect((await read.getChunk(testValue1Chunk.hash))?.data).to.be.undefined;
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
    expect((await read.getChunk(testValue4Chunk.hash))?.data).to.be.undefined;
    expect((await read.getChunk(testValue5Chunk.hash))?.data).to.be.undefined;
  });
});

test('cache eviction updates ref counts and removes cache chunks when their ref count goes to zero', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest({
    cacheSizeLimit: 300,
  });
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = {name: 'testValue3', size: 200},
    testValue4 = 'testValue4';
  //    4
  //  / |
  //  3 2
  //    |
  //    1
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk, testValue4Chunk} =
    await sourceStore.withWrite(async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      const testValue2Chunk = write.createChunk(testValue2, [
        testValue1Chunk.hash,
      ]);
      await write.putChunk(testValue2Chunk);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      const testValue4Chunk = write.createChunk(testValue4, [
        testValue2Chunk.hash,
        testValue3Chunk.hash,
      ]);
      await write.putChunk(testValue4Chunk);
      await write.setHead('testHeadSource', testValue4Chunk.hash);
      await write.commit();
      return {
        testValue1Chunk,
        testValue2Chunk,
        testValue3Chunk,
        testValue4Chunk,
      };
    });

  await lazyStore.withWrite(async write => {
    await write.setHead('testHeadLazy', testValue4Chunk.hash);
    await write.commit();
  });

  await lazyStore.withRead(async read => {
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
    // To make room for 3 (of size 200), 2 chunks of size 100 need to be
    // removed from cache.  2 and 4 are least recently used.  However,
    // first 2 is evicted, lowering 1's ref count to zero and is removed.
    // 4 is not removed since evicting 1 and processing gc from this eviction
    // made enough room
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });

  // gc chunks from base store
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource');
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    // testValue1Chunk and testValue2Chunk were evicted and are no longer available in base store
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
    expect(await read.getChunk(testValue2Chunk.hash)).to.be.undefined;
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
    expect((await read.getChunk(testValue4Chunk.hash))?.data).to.deep.equal(
      testValue4,
    );
  });
});

test('temp chunks are not evicted when cache size is exceeded', async () => {
  const {sourceStore, lazyStore} = createLazyStoreForTest();
  const testValue1 = 'testValue1',
    testValue2 = 'testValue2',
    testValue3 = 'testValue3';
  const {testValue1Chunk, testValue2Chunk, testValue3Chunk} =
    await sourceStore.withWrite(async write => {
      const testValue1Chunk = write.createChunk(testValue1, []);
      await write.putChunk(testValue1Chunk);
      await write.setHead('testHeadSource1', testValue1Chunk.hash);
      const testValue2Chunk = write.createChunk(testValue2, []);
      await write.putChunk(testValue2Chunk);
      await write.setHead('testHeadSource2', testValue2Chunk.hash);
      const testValue3Chunk = write.createChunk(testValue3, []);
      await write.putChunk(testValue3Chunk);
      await write.setHead('testHeadSource3', testValue3Chunk.hash);
      await write.commit();
      return {testValue1Chunk, testValue2Chunk, testValue3Chunk};
    });
  const tempValue1 = 'tempValue1',
    tempValue2 = 'tempValue2';
  const {tempValue1Chunk, tempValue2Chunk} = await lazyStore.withWrite(
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

      await write.commit();
      return {tempValue1Chunk, tempValue2Chunk};
    },
  );

  await lazyStore.withRead(async read => {
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
    // over cache size limit, should evict testValue1Chunk, but not any temp chunks
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });

  // gc chunks from base store
  await sourceStore.withWrite(async write => {
    await write.removeHead('testHeadSource1');
    await write.removeHead('testHeadSource2');
    await write.removeHead('testHeadSource3');
    await write.commit();
  });

  await lazyStore.withRead(async read => {
    // temp chunks were not evicted
    expect((await read.getChunk(tempValue1Chunk.hash))?.data).to.equal(
      tempValue1,
    );
    expect((await read.getChunk(tempValue2Chunk.hash))?.data).to.equal(
      tempValue2,
    );
    // testValue1Chunk was evicted and is no longer available in base store
    expect(await read.getChunk(testValue1Chunk.hash)).to.be.undefined;
    expect((await read.getChunk(testValue2Chunk.hash))?.data).to.equal(
      testValue2,
    );
    expect((await read.getChunk(testValue3Chunk.hash))?.data).to.equal(
      testValue3,
    );
  });
});

test('[all temp chunks] chunk ref counts are updated on commit and are deleted when their ref count goes to zero', async () => {
  const {lazyStore} = createLazyStoreForTest();

  //    R
  //  / |
  //  A B
  //  \ |
  //    C
  //    |
  //    D

  const {r, a, b, c, d} = await lazyStore.withWrite(async write => {
    const d = write.createChunk('d', []);
    const c = write.createChunk('c', [d.hash]);
    const b = write.createChunk('b', [c.hash]);
    const a = write.createChunk('a', [c.hash]);
    const r = write.createChunk('r', [a.hash, b.hash]);
    await write.putChunk(r);
    await write.putChunk(a);
    await write.putChunk(b);
    await write.putChunk(c);
    await write.putChunk(d);
    await write.setHead('test', r.hash);
    await write.commit();
    return {r, a, b, c, d};
  });

  expect(lazyStore.refCountsSnapshot).to.deep.equal(
    new Map([
      [r.hash, 1],
      [a.hash, 1],
      [b.hash, 1],
      [c.hash, 2],
      [d.hash, 1],
    ]),
  );

  // E
  // |
  // D

  const e = await lazyStore.withWrite(async write => {
    const e = write.createChunk('e', [d.hash]);
    await write.putChunk(e);
    await write.setHead('test', e.hash);
    await write.commit();
    return e;
  });

  expect(lazyStore.refCountsSnapshot).to.deep.equal(
    new Map([
      [d.hash, 1],
      [e.hash, 1],
    ]),
  );

  await lazyStore.withRead(async read => {
    expect(await read.getChunk(r.hash)).to.be.undefined;
    expect(await read.getChunk(a.hash)).to.be.undefined;
    expect(await read.getChunk(b.hash)).to.be.undefined;
    expect(await read.getChunk(c.hash)).to.be.undefined;
    expect(await read.getChunk(d.hash)).to.deep.equal(d);
    expect(await read.getChunk(e.hash)).to.deep.equal(e);
  });
});

test('[all cached chunks] chunk ref counts are updated on commit and are deleted when their ref count goes to zero', async () => {
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

  const {r, a, b, c, d} = await sourceStore.withWrite(async write => {
    const d = write.createChunk('d', []);
    const c = write.createChunk('c', [d.hash]);
    const b = write.createChunk('b', [c.hash]);
    const a = write.createChunk('a', [c.hash]);
    const r = write.createChunk('r', [a.hash, b.hash]);
    await write.putChunk(r);
    await write.putChunk(a);
    await write.putChunk(b);
    await write.putChunk(c);
    await write.putChunk(d);
    await write.setHead('testSource', r.hash);
    await write.commit();
    return {r, a, b, c, d};
  });

  await lazyStore.withWrite(async write => {
    await write.putChunk(r);
    await write.putChunk(a);
    await write.putChunk(b);
    await write.putChunk(c);
    await write.putChunk(d);
    await write.setHead('testLazy', r.hash);
    await write.commit();
  });

  expect(lazyStore.refCountsSnapshot).to.deep.equal(
    new Map([
      [r.hash, 1],
      [a.hash, 1],
      [b.hash, 1],
      [c.hash, 2],
      [d.hash, 1],
    ]),
  );

  // E
  // |
  // D

  const e = await sourceStore.withWrite(async write => {
    const e = write.createChunk('e', [d.hash]);
    await write.putChunk(e);
    await write.setHead('testSource', e.hash);
    await write.commit();
    return e;
  });

  await lazyStore.withWrite(async write => {
    await write.putChunk(e);
    await write.setHead('testLazy', e.hash);
    await write.commit();
    return e;
  });

  expect(lazyStore.refCountsSnapshot).to.deep.equal(
    new Map([
      [d.hash, 1],
      [e.hash, 1],
    ]),
  );

  await lazyStore.withRead(async read => {
    expect(await read.getChunk(r.hash)).to.be.undefined;
    expect(await read.getChunk(a.hash)).to.be.undefined;
    expect(await read.getChunk(b.hash)).to.be.undefined;
    expect(await read.getChunk(c.hash)).to.be.undefined;
    expect(await read.getChunk(d.hash)).to.deep.equal(d);
    expect(await read.getChunk(e.hash)).to.deep.equal(e);
  });
});

test('[mix of temp and cached chunks] chunk ref counts are updated on commit and are deleted when their ref count goes to zero', async () => {
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

  const {r, a, b, c, d} = await sourceStore.withWrite(async write => {
    const d = write.createChunk('d', []);
    const c = write.createChunk('c', [d.hash]);
    const b = write.createChunk('b', [c.hash]);
    const a = write.createChunk('a', [c.hash]);
    const r = write.createChunk('r', [a.hash, b.hash]);
    await write.putChunk(r);
    await write.putChunk(a);
    await write.putChunk(b);
    await write.putChunk(c);
    await write.putChunk(d);
    await write.setHead('testSource', r.hash);
    await write.commit();
    return {r, a, b, c, d};
  });

  await lazyStore.withWrite(async write => {
    await write.putChunk(r);
    await write.putChunk(a);
    await write.putChunk(b);
    await write.putChunk(c);
    await write.putChunk(d);
    await write.setHead('testLazy', r.hash);
    await write.commit();
  });

  expect(lazyStore.refCountsSnapshot).to.deep.equal(
    new Map([
      [r.hash, 1],
      [a.hash, 1],
      [b.hash, 1],
      [c.hash, 2],
      [d.hash, 1],
    ]),
  );

  // tempE
  // |
  // D

  const tempE = await lazyStore.withWrite(async write => {
    const tempE = write.createChunk('e', [d.hash]);
    await write.putChunk(tempE);
    await write.setHead('testLazy', tempE.hash);
    await write.commit();
    return tempE;
  });

  expect(lazyStore.refCountsSnapshot).to.deep.equal(
    new Map([
      [d.hash, 1],
      [tempE.hash, 1],
    ]),
  );

  // E
  // |
  // D
  const e = await sourceStore.withWrite(async write => {
    const e = write.createChunk('e', [d.hash]);
    await write.putChunk(e);
    await write.setHead('testSource', e.hash);
    await write.commit();
    return e;
  });

  await lazyStore.withWrite(async write => {
    await write.putChunk(e);
    await write.setHead('testLazy', e.hash);
    await write.commit();
    return e;
  });

  expect(lazyStore.refCountsSnapshot).to.deep.equal(
    new Map([
      [d.hash, 1],
      [e.hash, 1],
    ]),
  );

  await lazyStore.withRead(async read => {
    expect(await read.getChunk(r.hash)).to.be.undefined;
    expect(await read.getChunk(a.hash)).to.be.undefined;
    expect(await read.getChunk(b.hash)).to.be.undefined;
    expect(await read.getChunk(c.hash)).to.be.undefined;
    expect(await read.getChunk(tempE.hash)).to.be.undefined;
    expect(await read.getChunk(d.hash)).to.deep.equal(d);
    expect(await read.getChunk(e.hash)).to.deep.equal(e);
  });
});
