import {expect} from 'chai';
import {assert} from 'shared/src/asserts.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import type {Read, Store, Write} from '../dag/store.js';
import {TestStore} from '../dag/test-store.js';
import {ChainBuilder} from '../db/test-helpers.js';
import {FormatVersion} from '../format-version.js';
import {FrozenJSONValue, deepFreeze} from '../frozen-json.js';
import {Hash, emptyHash, makeNewFakeHashFunction} from '../hash.js';
import {getSizeOfEntry, getSizeOfValue} from '../size-of-value.js';
import {withRead, withWrite} from '../with-transactions.js';
import {
  DataNode,
  Diff,
  Entry,
  InternalNode,
  NODE_ENTRIES,
  NODE_LEVEL,
  findLeaf,
  makeNodeChunkData,
  parseBTreeNode,
  partition,
  toChunkData,
} from './node.js';
import {BTreeRead, NODE_HEADER_SIZE} from './read.js';
import {BTreeWrite} from './write.js';

suite('btree node', () => {
  function createSizedEntry<K, V>(
    key: K,
    value: V,
  ): [key: K, value: V, sizeOfEntry: number] {
    return [key, value, getEntrySize(key, value)];
  }

  type TreeData = {
    $level: number;
    [key: string]: TreeData | ReadonlyJSONValue;
  };

  function makeTree(
    node: TreeData,
    dagStore: Store,
    formatVersion: FormatVersion,
  ): Promise<Hash> {
    return withWrite(dagStore, async dagWrite => {
      const [h] = await makeTreeInner(node, dagWrite);
      await dagWrite.setHead('test', h);
      return h;
    });

    async function makeTreeInner(
      node: TreeData,
      dagWrite: Write,
    ): Promise<[Hash, number]> {
      const entries: [string, ReadonlyJSONValue | string][] = Object.entries(
        node,
      ).filter(e => e[0] !== '$level');
      if (node.$level === 0) {
        const dataNode = makeNodeChunkData(
          0,
          entries.map(entry => createSizedEntry(...entry)),
          formatVersion,
        );
        const chunk = dagWrite.createChunk(dataNode, []);
        await dagWrite.putChunk(chunk);
        return [chunk.hash, 0];
      }

      let level = 0;
      const ps = entries.map(async ([key, child]) => {
        const [hash, lvl] = await makeTreeInner(child as TreeData, dagWrite);
        level = Math.max(level, lvl);
        return createSizedEntry(key, hash);
      });
      const entries2 = await Promise.all(ps);

      const internalNode = makeNodeChunkData(
        level + 1,
        entries2,
        formatVersion,
      );
      const refs = entries2.map(pair => pair[1]);
      const chunk = dagWrite.createChunk(internalNode, refs);
      await dagWrite.putChunk(chunk);
      return [chunk.hash, level + 1];
    }
  }

  async function readTreeData(
    rootHash: Hash,
    dagRead: Read,
    formatVersion: FormatVersion,
  ): Promise<Record<string, unknown>> {
    const chunk = await dagRead.getChunk(rootHash);
    const node = parseBTreeNode(chunk?.data, formatVersion, getEntrySize);
    let lastKey: string | undefined;
    const rv: Record<string, unknown> = {
      $level: node[NODE_LEVEL],
    };

    if (node[NODE_LEVEL] === 0) {
      for (const [k, v] of node[NODE_ENTRIES]) {
        if (lastKey !== undefined) {
          assert(lastKey < k);
          lastKey = k;
        }
        rv[k] = v;
      }
      return rv;
    }

    for (const [k, hash] of (node as InternalNode)[NODE_ENTRIES]) {
      if (lastKey !== undefined) {
        expect(lastKey < k);
        lastKey = k;
      }
      rv[k] = await readTreeData(hash, dagRead, formatVersion);
    }
    return rv;
  }

  async function expectTree(
    rootHash: Hash,
    dagStore: Store,
    formatVersion: FormatVersion,
    expected: TreeData,
  ) {
    await withRead(dagStore, async dagRead => {
      expect(
        await readTreeData(rootHash, dagRead, formatVersion),
      ).to.deep.equal(expected);
    });
  }

  let minSize: number;
  let maxSize: number;
  let getEntrySize: <K, V>(k: K, v: V) => number;
  let chunkHeaderSize: number;

  setup(() => {
    minSize = 2;
    maxSize = 4;
    getEntrySize = () => 1;
    chunkHeaderSize = 0;
  });

  function doRead<R>(
    rootHash: Hash,
    dagStore: Store,
    formatVersion: FormatVersion,
    fn: (r: BTreeRead) => R | Promise<R>,
  ): Promise<R> {
    return withRead(dagStore, dagWrite => {
      const r = new BTreeRead(
        dagWrite,
        formatVersion,
        rootHash,
        getEntrySize,
        chunkHeaderSize,
      );
      return fn(r);
    });
  }

  function doWrite(
    rootHash: Hash,
    dagStore: Store,
    formatVersion: FormatVersion,
    fn: (w: BTreeWrite) => void | Promise<void>,
  ): Promise<Hash> {
    return withWrite(dagStore, async dagWrite => {
      const w = new BTreeWrite(
        dagWrite,
        formatVersion,
        rootHash,
        minSize,
        maxSize,
        getEntrySize,
        chunkHeaderSize,
      );
      await fn(w);
      const h = await w.flush();
      await dagWrite.setHead('test', h);
      return h;
    });
  }

  async function asyncIterToArray<T>(iter: AsyncIterable<T>): Promise<T[]> {
    const rv: T[] = [];
    for await (const e of iter) {
      rv.push(e);
    }
    return rv;
  }

  for (const formatVersion of [FormatVersion.V6, FormatVersion.V7] as const) {
    test(`findLeaf > v${formatVersion}`, async () => {
      const dagStore = new TestStore();

      const leaf0 = makeNodeChunkData(
        0,
        [
          createSizedEntry('a', 0),
          createSizedEntry('b', 1),
          createSizedEntry('c', 2),
        ],
        formatVersion,
      );

      const leaf1 = makeNodeChunkData(
        0,
        [
          createSizedEntry('d', 3),
          createSizedEntry('e', 4),
          createSizedEntry('f', 5),
        ],
        formatVersion,
      );
      const leaf2: DataNode = makeNodeChunkData(
        0,
        [
          createSizedEntry('g', 6),
          createSizedEntry('h', 7),
          createSizedEntry('i', 8),
        ],
        formatVersion,
      );

      let h0: Hash, h1: Hash, h2: Hash;

      let root: InternalNode;
      let rootHash: Hash;

      await withWrite(dagStore, async dagWrite => {
        const c0 = dagWrite.createChunk(leaf0, []);
        const c1 = dagWrite.createChunk(leaf1, []);
        const c2 = dagWrite.createChunk(leaf2, []);

        h0 = c0.hash;
        h1 = c1.hash;
        h2 = c2.hash;

        root = makeNodeChunkData(
          1,
          [
            createSizedEntry('c', h0),
            createSizedEntry('f', h1),
            createSizedEntry('i', h2),
          ],
          formatVersion,
        );

        const rootChunk = dagWrite.createChunk(root, [h0, h1, h2]);
        rootHash = rootChunk.hash;

        await dagWrite.putChunk(c0);
        await dagWrite.putChunk(c1);
        await dagWrite.putChunk(c2);
        await dagWrite.putChunk(rootChunk);
        await dagWrite.setHead('test', rootHash);
      });

      await withRead(dagStore, async dagRead => {
        const source = new BTreeRead(
          dagRead,
          formatVersion,
          rootHash,
          getEntrySize,
          chunkHeaderSize,
        );

        const t = async (
          key: string,
          hash: Hash,
          source: BTreeRead,
          expected: DataNode,
        ) => {
          const actual = await findLeaf(key, hash, source, source.rootHash);
          expect(toChunkData(actual, formatVersion)).to.deep.equal(expected);
        };

        await t('b', h0, source, leaf0);
        await t('a', h0, source, leaf0);
        await t('c', h0, source, leaf0);

        await t('a', rootHash, source, leaf0);
        await t('b', rootHash, source, leaf0);
        await t('c', rootHash, source, leaf0);
        await t('d', rootHash, source, leaf1);
        await t('e', rootHash, source, leaf1);
        await t('f', rootHash, source, leaf1);
        await t('g', rootHash, source, leaf2);
        await t('h', rootHash, source, leaf2);
        await t('i', rootHash, source, leaf2);
      });
    });

    test(`empty read tree > v${formatVersion}`, async () => {
      const dagStore = new TestStore();
      await withRead(dagStore, async dagRead => {
        const r = new BTreeRead(dagRead, formatVersion);
        expect(await r.get('a')).to.be.undefined;
        expect(await r.has('b')).to.be.false;
        expect(await asyncIterToArray(r.scan(''))).to.deep.equal([]);
      });
    });

    test(`empty write tree > v${formatVersion}`, async () => {
      const chunkHasher = makeNewFakeHashFunction();
      const dagStore = new TestStore(undefined, chunkHasher);

      const emptyTreeHash = chunkHasher();

      await withWrite(dagStore, async dagWrite => {
        const w = new BTreeWrite(
          dagWrite,
          formatVersion,
          undefined,
          minSize,
          maxSize,
          getEntrySize,
          chunkHeaderSize,
        );
        expect(await w.get('a')).to.be.undefined;
        expect(await w.has('b')).to.be.false;
        expect(await asyncIterToArray(w.scan(''))).to.deep.equal([]);

        const h = await w.flush();
        expect(h).to.equal('face0000000040008000000000000000' + '000000000001');
      });
      let rootHash = await withWrite(dagStore, async dagWrite => {
        const w = new BTreeWrite(
          dagWrite,
          formatVersion,
          undefined,
          minSize,
          maxSize,
          getEntrySize,
          chunkHeaderSize,
        );
        await w.put('a', 1);
        const h = await w.flush();
        expect(h).to.not.equal(emptyHash);
        expect(h).to.not.equal(emptyTreeHash);
        await dagWrite.setHead('test', h);
        return h;
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        expect(await w.del('a')).to.be.true;
      });

      // We do not restore back to empty hash when empty.
      expect(rootHash).to.not.equal(emptyHash);
      expect(rootHash).to.equal(
        'face0000000040008000000000000000' + '000000000003',
      );
    });

    test(`get > v${formatVersion}`, async () => {
      const dagStore = new TestStore();

      const tree: TreeData = {
        $level: 1,
        f: {
          $level: 0,
          b: 0,
          d: 1,
          f: 2,
        },
        l: {
          $level: 0,
          h: 3,
          j: 4,
          l: 5,
        },
        r: {
          $level: 0,
          n: 6,
          p: 7,
          r: 8,
        },
      };

      const rootHash = await makeTree(tree, dagStore, formatVersion);

      await withRead(dagStore, async dagRead => {
        const source = new BTreeRead(
          dagRead,
          formatVersion,
          rootHash,
          getEntrySize,
          chunkHeaderSize,
        );

        expect(await source.get('b')).to.equal(0);
        expect(await source.get('d')).to.equal(1);
        expect(await source.get('f')).to.equal(2);
        expect(await source.get('h')).to.equal(3);
        expect(await source.get('j')).to.equal(4);
        expect(await source.get('l')).to.equal(5);
        expect(await source.get('n')).to.equal(6);
        expect(await source.get('p')).to.equal(7);
        expect(await source.get('r')).to.equal(8);

        expect(await source.get('a')).to.equal(undefined);
        expect(await source.get('c')).to.equal(undefined);
        expect(await source.get('e')).to.equal(undefined);
        expect(await source.get('g')).to.equal(undefined);
        expect(await source.get('i')).to.equal(undefined);
        expect(await source.get('k')).to.equal(undefined);
        expect(await source.get('m')).to.equal(undefined);
        expect(await source.get('o')).to.equal(undefined);
        expect(await source.get('q')).to.equal(undefined);
        expect(await source.get('s')).to.equal(undefined);
      });
    });

    test(`has > v${formatVersion}`, async () => {
      const dagStore = new TestStore();

      const tree: TreeData = {
        $level: 1,
        f: {
          $level: 0,
          b: 0,
          d: 1,
          f: 2,
        },
        l: {
          $level: 0,
          h: 3,
          j: 4,
          l: 5,
        },
        r: {
          $level: 0,
          n: 6,
          p: 7,
          r: 8,
        },
      };

      const rootHash = await makeTree(tree, dagStore, formatVersion);

      await withRead(dagStore, async dagRead => {
        const source = new BTreeRead(
          dagRead,
          formatVersion,
          rootHash,
          getEntrySize,
          chunkHeaderSize,
        );

        expect(await source.has('b')).to.be.true;
        expect(await source.has('d')).to.be.true;
        expect(await source.has('f')).to.be.true;
        expect(await source.has('h')).to.be.true;
        expect(await source.has('j')).to.be.true;
        expect(await source.has('l')).to.be.true;
        expect(await source.has('n')).to.be.true;
        expect(await source.has('p')).to.be.true;
        expect(await source.has('r')).to.be.true;

        expect(await source.has('a')).to.be.false;
        expect(await source.has('c')).to.be.false;
        expect(await source.has('e')).to.be.false;
        expect(await source.has('g')).to.be.false;
        expect(await source.has('i')).to.be.false;
        expect(await source.has('k')).to.be.false;
        expect(await source.has('m')).to.be.false;
        expect(await source.has('o')).to.be.false;
        expect(await source.has('q')).to.be.false;
        expect(await source.has('s')).to.be.false;
      });
    });

    test(`partition`, () => {
      const getSize = (v: string) => v.length;

      const t = (input: string[], expected: string[][]) => {
        expect(partition(input, getSize, 2, 4)).to.deep.equal(expected);
      };

      t([], []);
      t(['a'], [['a']]);
      t(['a', 'b'], [['a', 'b']]);
      t(['a', 'b', 'c'], [['a', 'b', 'c']]);
      t(
        ['a', 'b', 'c', 'd'],
        [
          ['a', 'b'],
          ['c', 'd'],
        ],
      );
      t(
        ['a', 'b', 'c', 'd', 'e'],
        [
          ['a', 'b'],
          ['c', 'd', 'e'],
        ],
      );
      t(
        ['a', 'b', 'c', 'd', 'e', 'f'],
        [
          ['a', 'b'],
          ['c', 'd'],
          ['e', 'f'],
        ],
      );
      t(['ab'], [['ab']]);
      t(['ab', 'cd'], [['ab'], ['cd']]);
      t(['ab', 'cd', 'ef'], [['ab'], ['cd'], ['ef']]);
      t(['ab', 'cd', 'e'], [['ab'], ['cd', 'e']]);
      t(['ab', 'c', 'de'], [['ab'], ['c', 'de']]);
      t(['a', 'bc', 'de'], [['a', 'bc'], ['de']]);
      t(['abc', 'de'], [['abc'], ['de']]);
      t(['abc', 'def'], [['abc'], ['def']]);
      t(['a', 'bcd', 'e'], [['a', 'bcd'], ['e']]);
      t(['ab', 'cde', 'f'], [['ab'], ['cde', 'f']]);
      t(['abc', 'd', 'efg'], [['abc'], ['d', 'efg']]);
      t(['abcd', 'e', 'f'], [['abcd'], ['e', 'f']]);
      t(['a', 'bcde', 'f'], [['a'], ['bcde'], ['f']]);
      t(['a', 'bcdef', 'g'], [['a'], ['bcdef'], ['g']]);
    });

    test(`put > v${formatVersion}`, async () => {
      const dagStore = new TestStore();

      const tree: TreeData = {
        $level: 0,
        b: 0,
        d: 1,
        f: 2,
      };

      let rootHash = await makeTree(tree, dagStore, formatVersion);

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        await w.put('a', 'aaa');

        expect(await w.get('a')).to.equal('aaa');
        expect(await w.get('b')).to.equal(0);
        await w.put('b', 'bbb');
        expect(await w.get('b')).to.equal('bbb');
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 0,
        a: 'aaa',
        b: 'bbb',
        d: 1,
        f: 2,
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        await w.put('c', 'ccc');
        expect(await w.get('a')).to.equal('aaa');
        expect(await w.get('b')).to.equal('bbb');
        expect(await w.get('c')).to.equal('ccc');
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 1,
        b: {
          $level: 0,
          a: 'aaa',
          b: 'bbb',
        },
        f: {
          $level: 0,
          c: 'ccc',
          d: 1,
          f: 2,
        },
      });

      async function write(data: Record<string, ReadonlyJSONValue>) {
        rootHash = await withWrite(dagStore, async dagWrite => {
          const w = new BTreeWrite(
            dagWrite,
            formatVersion,
            rootHash,
            minSize,
            maxSize,
            getEntrySize,
            chunkHeaderSize,
          );
          for (const [k, v] of Object.entries(data)) {
            await w.put(k, deepFreeze(v));
            expect(await w.get(k)).to.equal(v);
            expect(await w.has(k)).to.equal(true);
          }
          const h = await w.flush();
          for (const [k, v] of Object.entries(data)) {
            expect(await w.get(k)).to.equal(v);
            expect(await w.has(k)).to.equal(true);
          }

          await dagWrite.setHead('test', h);

          for (const [k, v] of Object.entries(data)) {
            expect(await w.get(k)).to.equal(v);
            expect(await w.has(k)).to.equal(true);
          }

          return h;
        });
      }

      await write({
        e: 'eee',
        f: 'fff',
        g: 'ggg',
        h: 'hhh',
        i: 'iii',
        j: 'jjj',
      });
      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 1,
        b: {
          $level: 0,
          a: 'aaa',
          b: 'bbb',
        },
        d: {
          $level: 0,
          c: 'ccc',
          d: 1,
        },
        f: {
          $level: 0,
          e: 'eee',
          f: 'fff',
        },
        j: {
          $level: 0,
          g: 'ggg',
          h: 'hhh',
          i: 'iii',
          j: 'jjj',
        },
      });

      await write({
        k: 'kkk',
      });
      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 2,
        d: {
          $level: 1,
          b: {
            $level: 0,
            a: 'aaa',
            b: 'bbb',
          },
          d: {
            $level: 0,
            c: 'ccc',
            d: 1,
          },
        },
        k: {
          $level: 1,
          f: {
            $level: 0,
            e: 'eee',
            f: 'fff',
          },
          h: {
            $level: 0,
            g: 'ggg',
            h: 'hhh',
          },
          k: {
            $level: 0,
            i: 'iii',
            j: 'jjj',
            k: 'kkk',
          },
        },
      });

      await write({
        q: 'qqq',
        m: 'mmm',
        l: 'lll',
        p: 'ppp',
        o: 'ooo',
        n: 'nnn',
      });
      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 2,
        d: {
          $level: 1,
          b: {
            $level: 0,
            a: 'aaa',
            b: 'bbb',
          },
          d: {
            $level: 0,
            c: 'ccc',
            d: 1,
          },
        },
        h: {
          $level: 1,
          f: {
            $level: 0,
            e: 'eee',
            f: 'fff',
          },
          h: {
            $level: 0,
            g: 'ggg',
            h: 'hhh',
          },
        },
        q: {
          $level: 1,
          j: {
            $level: 0,
            i: 'iii',
            j: 'jjj',
          },
          l: {
            $level: 0,
            k: 'kkk',
            l: 'lll',
          },
          n: {
            $level: 0,
            m: 'mmm',
            n: 'nnn',
          },
          q: {
            $level: 0,
            o: 'ooo',
            p: 'ppp',
            q: 'qqq',
          },
        },
      });

      await write({
        boo: '👻',
      });
      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 2,
        d: {
          $level: 1,
          b: {
            $level: 0,
            a: 'aaa',
            b: 'bbb',
          },
          d: {
            $level: 0,
            boo: '👻',
            c: 'ccc',
            d: 1,
          },
        },
        h: {
          $level: 1,
          f: {
            $level: 0,
            e: 'eee',
            f: 'fff',
          },
          h: {
            $level: 0,
            g: 'ggg',
            h: 'hhh',
          },
        },
        q: {
          $level: 1,
          j: {
            $level: 0,
            i: 'iii',
            j: 'jjj',
          },
          l: {
            $level: 0,
            k: 'kkk',
            l: 'lll',
          },
          n: {
            $level: 0,
            m: 'mmm',
            n: 'nnn',
          },
          q: {
            $level: 0,
            o: 'ooo',
            p: 'ppp',
            q: 'qqq',
          },
        },
      });

      await write({
        bx: true,
        bx2: false,
      });
      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 2,
        d: {
          $level: 1,
          b: {
            $level: 0,
            a: 'aaa',
            b: 'bbb',
          },
          bx: {
            $level: 0,
            boo: '👻',
            bx: true,
          },
          d: {
            $level: 0,
            bx2: false,
            c: 'ccc',
            d: 1,
          },
        },
        h: {
          $level: 1,
          f: {
            $level: 0,
            e: 'eee',
            f: 'fff',
          },
          h: {
            $level: 0,
            g: 'ggg',
            h: 'hhh',
          },
        },
        q: {
          $level: 1,
          j: {
            $level: 0,
            i: 'iii',
            j: 'jjj',
          },
          l: {
            $level: 0,
            k: 'kkk',
            l: 'lll',
          },
          n: {
            $level: 0,
            m: 'mmm',
            n: 'nnn',
          },
          q: {
            $level: 0,
            o: 'ooo',
            p: 'ppp',
            q: 'qqq',
          },
        },
      });
    });

    test(`del - single data node > v${formatVersion}`, async () => {
      const dagStore = new TestStore();

      const tree: TreeData = {
        $level: 0,
        b: 0,
        d: 1,
        f: 2,
      };

      let rootHash = await makeTree(tree, dagStore, formatVersion);

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        expect(await w.del('a')).to.equal(false);
        expect(await w.del('d')).to.equal(true);
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 0,
        b: 0,
        f: 2,
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        expect(await w.del('f')).to.equal(true);
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 0,
        b: 0,
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        expect(await w.del('b')).to.equal(true);
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 0,
      });
    });

    test(`del - flatten > v${formatVersion}`, async () => {
      const dagStore = new TestStore();

      // This tests that we can flatten "an invalid tree"

      {
        const tree: TreeData = {
          $level: 3,
          b: {
            $level: 2,
            b: {
              $level: 1,
              b: {
                $level: 0,
                a: 'aaa',
                b: 'bbb',
              },
            },
          },
        };

        let rootHash = await makeTree(tree, dagStore, formatVersion);

        rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
          expect(await w.del('a')).to.equal(true);
        });

        await expectTree(rootHash, dagStore, formatVersion, {
          $level: 0,
          b: 'bbb',
        });
      }

      {
        const tree: TreeData = {
          $level: 3,
          b: {
            $level: 2,
            b: {
              $level: 1,
              b: {
                $level: 0,
                a: 'aaa',
                b: 'bbb',
              },
            },
          },
        };

        let rootHash = await makeTree(tree, dagStore, formatVersion);

        rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
          expect(await w.del('b')).to.equal(true);
        });

        await expectTree(rootHash, dagStore, formatVersion, {
          $level: 0,
          a: 'aaa',
        });
      }
    });

    test(`del - with internal nodes > v${formatVersion}`, async () => {
      const dagStore = new TestStore();

      const tree: TreeData = {
        $level: 2,
        d: {
          $level: 1,
          b: {
            $level: 0,
            a: 'aaa',
            b: 'bbb',
          },
          d: {
            $level: 0,
            c: 'ccc',
            d: 'ddd',
          },
        },
        k: {
          $level: 1,
          f: {
            $level: 0,
            e: 'eee',
            f: 'fff',
          },
          h: {
            $level: 0,
            g: 'ggg',
            h: 'hhh',
          },
          k: {
            $level: 0,
            i: 'iii',
            j: 'jjj',
            k: 'kkk',
          },
        },
      };

      let rootHash = await makeTree(tree, dagStore, formatVersion);

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        expect(await w.del('k')).to.equal(true);
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 2,
        d: {
          $level: 1,
          b: {
            $level: 0,
            a: 'aaa',
            b: 'bbb',
          },
          d: {
            $level: 0,
            c: 'ccc',
            d: 'ddd',
          },
        },
        j: {
          $level: 1,
          f: {
            $level: 0,
            e: 'eee',
            f: 'fff',
          },
          h: {
            $level: 0,
            g: 'ggg',
            h: 'hhh',
          },
          j: {
            $level: 0,
            i: 'iii',
            j: 'jjj',
          },
        },
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        expect(await w.del('c')).to.equal(true);
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 2,
        f: {
          $level: 1,
          d: {
            $level: 0,
            a: 'aaa',
            b: 'bbb',
            d: 'ddd',
          },
          f: {
            $level: 0,
            e: 'eee',
            f: 'fff',
          },
        },
        j: {
          $level: 1,
          h: {
            $level: 0,
            g: 'ggg',
            h: 'hhh',
          },
          j: {
            $level: 0,
            i: 'iii',
            j: 'jjj',
          },
        },
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        expect(await w.del('e')).to.equal(true);
        expect(await w.del('f')).to.equal(true);
        expect(await w.del('g')).to.equal(true);
        expect(await w.del('h')).to.equal(true);
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 1,
        d: {
          $level: 0,
          a: 'aaa',
          b: 'bbb',
          d: 'ddd',
        },
        j: {
          $level: 0,
          i: 'iii',
          j: 'jjj',
        },
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        expect(await w.del('a')).to.equal(true);
        expect(await w.del('b')).to.equal(true);
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 0,
        d: 'ddd',
        i: 'iii',
        j: 'jjj',
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        expect(await w.del('i')).to.equal(true);
        expect(await w.del('j')).to.equal(true);
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 0,
        d: 'ddd',
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        expect(await w.del('d')).to.equal(true);
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 0,
      });
    });

    test(`put - invalid > v${formatVersion}`, async () => {
      const dagStore = new TestStore();

      // This tests that we can do puts on "an invalid tree"

      const tree: TreeData = {
        $level: 2,
        b: {
          $level: 2,
          b: {
            $level: 0,
            b: 'bbb',
          },
        },
      };

      let rootHash = await makeTree(tree, dagStore, formatVersion);

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        await w.put('c', 'ccc');
      });

      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 2,
        c: {
          $level: 1,
          c: {
            $level: 0,
            b: 'bbb',
            c: 'ccc',
          },
        },
      });
    });

    test(`put/del - getSize > v${formatVersion}`, async () => {
      minSize = 30;
      maxSize = minSize * 2;
      getEntrySize = (k, v) => getSizeOfEntry(k, v);

      const dagStore = new TestStore();

      // This tests that we can do puts on "an invalid tree"

      const tree: TreeData = {
        $level: 0,
      };

      let rootHash = await makeTree(tree, dagStore, formatVersion);

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        await w.put('aaaa', 'a1');
      });

      expect(getSizeOfValue('aaaa')).to.equal(9);
      expect(getSizeOfValue('a1')).to.equal(7);
      expect(getSizeOfEntry('aaaa', 'a1')).to.equal(27);
      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 0,
        aaaa: 'a1',
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        await w.put('c', '');
      });
      expect(getSizeOfEntry('c', '')).to.equal(22);
      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 0,
        aaaa: 'a1',
        c: '',
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        await w.put('b', 'b234');
      });
      expect(getSizeOfEntry('b', 'b234')).to.equal(26);
      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 1,

        b: {
          $level: 0,
          aaaa: 'a1',
          b: 'b234',
        },
        c: {
          $level: 0,
          c: '',
        },
      });

      rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
        await w.del('b');
      });
      await expectTree(rootHash, dagStore, formatVersion, {
        $level: 0,
        aaaa: 'a1',
        c: '',
      });
    });

    test(`scan > v${formatVersion}`, async () => {
      const t = async (
        entries: Entry<ReadonlyJSONValue>[],
        fromKey = '',
        expectedEntries = entries,
      ) => {
        const dagStore = new TestStore();

        const tree: TreeData = {
          $level: 0,
        };

        let rootHash = await makeTree(tree, dagStore, formatVersion);

        rootHash = await doWrite(rootHash, dagStore, formatVersion, async w => {
          for (const [k, v] of entries) {
            await w.put(k, deepFreeze(v));
          }
        });

        await doRead(rootHash, dagStore, formatVersion, async r => {
          const res: Entry<FrozenJSONValue>[] = [];
          const scanResult = r.scan(fromKey);
          for await (const e of scanResult) {
            res.push(e);
          }
          expect(res).to.deep.equal(expectedEntries);
        });
      };

      await t([]);
      await t([createSizedEntry('a', 1)]);
      await t([createSizedEntry('a', 1), createSizedEntry('b', 2)]);
      await t([
        createSizedEntry('a', 1),
        createSizedEntry('b', 2),
        createSizedEntry('c', 3),
      ]);
      await t([
        createSizedEntry('a', 1),
        createSizedEntry('b', 2),
        createSizedEntry('c', 3),
        createSizedEntry('d', 4),
      ]);
      await t([
        createSizedEntry('a', 1),
        createSizedEntry('b', 2),
        createSizedEntry('c', 3),
        createSizedEntry('d', 4),
        createSizedEntry('e', 5),
      ]);

      await t(
        [
          createSizedEntry('a', 0),
          createSizedEntry('aa', 1),
          createSizedEntry('aaa', 2),
          createSizedEntry('aab', 3),
          createSizedEntry('ab', 4),
          createSizedEntry('b', 5),
        ],
        'aa',
        [
          createSizedEntry('aa', 1),
          createSizedEntry('aaa', 2),
          createSizedEntry('aab', 3),
          createSizedEntry('ab', 4),
          createSizedEntry('b', 5),
        ],
      );

      await t(
        [
          createSizedEntry('a', 1),
          createSizedEntry('b', 2),
          createSizedEntry('c', 3),
          createSizedEntry('d', 4),
          createSizedEntry('e', 5),
        ],
        'f',
        [],
      );

      await t(
        [
          createSizedEntry('a', 1),
          createSizedEntry('b', 2),
          createSizedEntry('c', 3),
          createSizedEntry('d', 4),
          createSizedEntry('e', 5),
        ],
        'e',
        [createSizedEntry('e', 5)],
      );
    });

    test(`diff > v${formatVersion}`, async () => {
      const t = async (
        oldEntries: Entry<ReadonlyJSONValue>[],
        newEntries: Entry<ReadonlyJSONValue>[],
        expectedDiff: Diff,
      ) => {
        const dagStore = new TestStore();

        const [oldHash, newHash] = await withWrite(dagStore, async dagWrite => {
          const oldTree = new BTreeWrite(
            dagWrite,
            formatVersion,
            undefined,
            minSize,
            maxSize,
            getEntrySize,
            chunkHeaderSize,
          );
          for (const entry of oldEntries) {
            await oldTree.put(entry[0], deepFreeze(entry[1]));
          }

          const newTree = new BTreeWrite(
            dagWrite,
            formatVersion,
            undefined,
            minSize,
            maxSize,
            getEntrySize,
            chunkHeaderSize,
          );
          for (const entry of newEntries) {
            await newTree.put(entry[0], deepFreeze(entry[1]));
          }

          const oldHash = await oldTree.flush();
          const newHash = await newTree.flush();

          await dagWrite.setHead('test/old', oldHash);
          await dagWrite.setHead('test/new', newHash);

          return [oldHash, newHash];
        });

        await withRead(dagStore, async dagRead => {
          const oldTree = new BTreeRead(
            dagRead,
            formatVersion,
            oldHash,
            getEntrySize,
            chunkHeaderSize,
          );
          const newTree = new BTreeRead(
            dagRead,
            formatVersion,
            newHash,
            getEntrySize,
            chunkHeaderSize,
          );

          const actual = [];
          for await (const diffRes of newTree.diff(oldTree)) {
            actual.push(diffRes);
          }
          expect(actual).to.deep.equal(expectedDiff);
        });
      };

      await t([], [], []);

      await t(
        [createSizedEntry('a', 0)],
        [],
        [{op: 'del', key: 'a', oldValue: 0}],
      );
      await t(
        [],
        [createSizedEntry('a', 0)],
        [{op: 'add', key: 'a', newValue: 0}],
      );
      await t([createSizedEntry('a', 0)], [createSizedEntry('a', 0)], []);
      await t(
        [createSizedEntry('a', 0)],
        [createSizedEntry('a', 1)],
        [{op: 'change', key: 'a', oldValue: 0, newValue: 1}],
      );

      await t(
        [createSizedEntry('b', 1), createSizedEntry('d', 2)],
        [createSizedEntry('d', 2), createSizedEntry('f', 3)],
        [
          {op: 'del', key: 'b', oldValue: 1},
          {op: 'add', key: 'f', newValue: 3},
        ],
      );

      await t(
        [
          createSizedEntry('b', 1),
          createSizedEntry('d', 2),
          createSizedEntry('e', 4),
        ],
        [createSizedEntry('d', 22), createSizedEntry('f', 3)],
        [
          {op: 'del', key: 'b', oldValue: 1},
          {op: 'change', key: 'd', oldValue: 2, newValue: 22},
          {op: 'del', key: 'e', oldValue: 4},
          {op: 'add', key: 'f', newValue: 3},
        ],
      );

      await t(
        [
          createSizedEntry('b', 1),
          createSizedEntry('d', 2),
          createSizedEntry('e', 4),
          createSizedEntry('h', 5),
          createSizedEntry('i', 6),
          createSizedEntry('j', 7),
          createSizedEntry('k', 8),
          createSizedEntry('l', 9),
        ],
        [createSizedEntry('d', 22), createSizedEntry('f', 3)],
        [
          {op: 'del', key: 'b', oldValue: 1},
          {op: 'change', key: 'd', oldValue: 2, newValue: 22},
          {op: 'del', key: 'e', oldValue: 4},
          {op: 'add', key: 'f', newValue: 3},

          {op: 'del', key: 'h', oldValue: 5},
          {op: 'del', key: 'i', oldValue: 6},
          {op: 'del', key: 'j', oldValue: 7},
          {op: 'del', key: 'k', oldValue: 8},
          {op: 'del', key: 'l', oldValue: 9},
        ],
      );

      await t(
        [
          createSizedEntry('b', 1),
          createSizedEntry('b1', 11),
          createSizedEntry('d', 2),
          createSizedEntry('d1', 12),
          createSizedEntry('e', 4),
          createSizedEntry('e1', 14),
          createSizedEntry('h', 5),
          createSizedEntry('h1', 15),
          createSizedEntry('i', 6),
          createSizedEntry('i1', 16),
          createSizedEntry('j', 7),
          createSizedEntry('j1', 17),
          createSizedEntry('k', 8),
          createSizedEntry('k1', 18),
          createSizedEntry('l', 9),
          createSizedEntry('l1', 19),
        ],
        [
          createSizedEntry('l1', 19),
          createSizedEntry('l', 9),
          createSizedEntry('k1', 18),
          // createSizedEntry('k', 8),
          createSizedEntry('j1', 17),
          createSizedEntry('j', 7),
          createSizedEntry('i1', 16),
          createSizedEntry('i', 6),
          createSizedEntry('h1', 15),
          createSizedEntry('h', 5),
          createSizedEntry('e1', 141),
          createSizedEntry('e', 0),
          createSizedEntry('d1', 0),
          createSizedEntry('d', 0),
          createSizedEntry('b2', 0),
          createSizedEntry('b1', 0),
          createSizedEntry('b', 1),
        ],
        [
          {
            key: 'b1',
            newValue: 0,
            oldValue: 11,
            op: 'change',
          },
          {
            key: 'b2',
            newValue: 0,
            op: 'add',
          },
          {
            key: 'd',
            newValue: 0,
            oldValue: 2,
            op: 'change',
          },
          {
            key: 'd1',
            newValue: 0,
            oldValue: 12,
            op: 'change',
          },
          {
            key: 'e',
            newValue: 0,
            oldValue: 4,
            op: 'change',
          },
          {
            key: 'e1',
            newValue: 141,
            oldValue: 14,
            op: 'change',
          },
          {
            key: 'k',
            oldValue: 8,
            op: 'del',
          },
        ],
      );
    });

    test(`chunk header size`, () => {
      // This just ensures that the constant is correct.
      const chunkData = makeNodeChunkData(0, [], formatVersion);
      const entriesSize = getSizeOfValue(chunkData[NODE_ENTRIES]);
      const chunkSize = getSizeOfValue(chunkData);
      expect(chunkSize - entriesSize).to.equal(NODE_HEADER_SIZE);
    });
  }

  test('ChunkNotFound?', async () => {
    const dagStore = new TestStore();

    const tree: TreeData = {
      $level: 2,
      d: {
        $level: 1,
        b: {
          $level: 0,
          a: 'aaa',
          b: 'bbb',
        },
        d: {
          $level: 0,
          c: 'ccc',
          d: 'ddd',
        },
      },
      k: {
        $level: 1,
        f: {
          $level: 0,
          e: 'eee',
          f: 'fff',
        },
        h: {
          $level: 0,
          g: 'ggg',
          h: 'hhh',
        },
        k: {
          $level: 0,
          i: 'iii',
          j: 'jjj',
          k: 'kkk',
        },
      },
    };
    const rootHash = await makeTree(tree, dagStore, FormatVersion.Latest);

    await withWrite(dagStore, async dagWrite => {
      const tree = new BTreeWrite(
        dagWrite,
        FormatVersion.Latest,
        rootHash,
        minSize,
        maxSize,
        getEntrySize,
        chunkHeaderSize,
      );

      await tree.put('l', 'lll1');

      const ps = [];
      const putPromise = tree.put('l', 'lll2');

      for (let i = 0; i < 5; i++) {
        await Promise.resolve(1);
        ps.push(tree.get('l'));
      }

      await putPromise;

      expect(await Promise.all(ps)).deep.equal([
        'lll2',
        'lll2',
        'lll2',
        'lll2',
        'lll2',
      ]);
    });
  });
});

suite('Write nodes using ChainBuilder', () => {
  // This test ensures that we write the correct data chunks for btree nodes
  // depending in the replicache format version.

  function looksLikeBTreeChunk(
    v: unknown,
  ): v is readonly [level: number, entries: unknown[]] {
    return (
      Array.isArray(v) &&
      v.length === 2 &&
      typeof v[0] === 'number' &&
      Array.isArray(v[1])
    );
  }

  const getBTreeNodes = async (formatVersion: FormatVersion) => {
    const dagStore = new TestStore();
    const clientID = 'client1';
    const b = new ChainBuilder(dagStore, undefined, formatVersion);
    await b.addGenesis(clientID);
    await b.addLocal(clientID, [['a', 'a']]);
    await b.addLocal(clientID, [['b', 'bb']]);

    return dagStore
      .chunks()
      .map(c => c.data)
      .filter(looksLikeBTreeChunk);
  };

  test('v6', async () => {
    expect(await getBTreeNodes(FormatVersion.V6)).to.deep.equal([
      [0, []],
      [0, [['a', 'a']]],
      [
        0,
        [
          ['a', 'a'],
          ['b', 'bb'],
        ],
      ],
    ]);
  });

  test('v7', async () => {
    expect(await getBTreeNodes(FormatVersion.V7)).to.deep.equal([
      [0, []],
      [0, [['a', 'a', 23]]],
      [
        0,
        [
          ['a', 'a', 23],
          ['b', 'bb', 24],
        ],
      ],
    ]);
  });
});
