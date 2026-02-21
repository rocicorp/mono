import {bench, describe} from 'vitest';
import {getSizeOfEntry} from '../../../shared/src/size-of-value.ts';
import {TestStore} from '../dag/test-store.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {type FrozenJSONValue, deepFreeze} from '../frozen-json.ts';
import {emptyHash} from '../hash.ts';
import {withWrite} from '../with-transactions.ts';
import {NODE_HEADER_SIZE} from './read.ts';
import {BTreeWrite} from './write.ts';

describe('BTreeWrite bulk load performance', () => {
  const formatVersion = FormatVersion.Latest;
  const minSize = 8 * 1024;
  const maxSize = 16 * 1024;

  function generateEntries(
    count: number,
    valueSize: 'small' | 'large',
  ): Array<[string, FrozenJSONValue]> {
    const entries: Array<[string, FrozenJSONValue]> = [];
    for (let i = 0; i < count; i++) {
      const key = `key${i.toString().padStart(6, '0')}`;
      const value =
        valueSize === 'small'
          ? `value${i}`
          : {
              id: i,
              name: `name${i}`,
              description: `This is a longer description for entry ${i}`,
              metadata: {
                created: Date.now(),
                tags: ['tag1', 'tag2', 'tag3'],
                nested: {
                  level1: {
                    level2: {
                      data: `nested-${i}`,
                    },
                  },
                },
              },
            };
      entries.push([key, deepFreeze(value)]);
    }
    return entries;
  }

  for (const count of [100, 1000, 10000]) {
    for (const valueSize of ['small', 'large'] as const) {
      describe(`${count} entries with ${valueSize} values`, () => {
        bench(`sequential put() - ${count} ${valueSize}`, async () => {
          const dagStore = new TestStore();
          const entries = generateEntries(count, valueSize);

          await withWrite(dagStore, async dagWrite => {
            const tree = new BTreeWrite(
              dagWrite,
              formatVersion,
              emptyHash,
              minSize,
              maxSize,
              getSizeOfEntry,
              NODE_HEADER_SIZE,
            );
            for (const [key, value] of entries) {
              await tree.put(key, value);
            }
            await tree.flush();
          });
        });

        bench(`putMany() - ${count} ${valueSize}`, async () => {
          const dagStore = new TestStore();
          const entries = generateEntries(count, valueSize);

          await withWrite(dagStore, async dagWrite => {
            const tree = new BTreeWrite(
              dagWrite,
              formatVersion,
              emptyHash,
              minSize,
              maxSize,
              getSizeOfEntry,
              NODE_HEADER_SIZE,
            );
            await tree.putMany(entries);
            await tree.flush();
          });
        });
      });
    }
  }

  // Separate benchmark group for existing tree operations
  describe('putMany on existing tree', () => {
    for (const count of [100, 1000, 10000]) {
      for (const valueSize of ['small', 'large'] as const) {
        bench(
          `putMany() on existing tree - ${count} ${valueSize}`,
          async () => {
            const dagStore = new TestStore();
            const entries = generateEntries(count, valueSize);
            const newEntries = generateEntries(count, valueSize).map(
              ([k, v]) => [`new_${k}`, v] as [string, FrozenJSONValue],
            );

            // Pre-populate tree
            const hash = await withWrite(dagStore, async dagWrite => {
              const tree = new BTreeWrite(
                dagWrite,
                formatVersion,
                emptyHash,
                minSize,
                maxSize,
                getSizeOfEntry,
                NODE_HEADER_SIZE,
              );
              await tree.putMany(entries);
              const h = await tree.flush();
              await dagWrite.setHead('main', h);
              return h;
            });

            // Now insert new entries into existing tree
            await withWrite(dagStore, async dagWrite => {
              const tree = new BTreeWrite(
                dagWrite,
                formatVersion,
                hash,
                minSize,
                maxSize,
                getSizeOfEntry,
                NODE_HEADER_SIZE,
              );
              await tree.putMany(newEntries);
              await tree.flush();
            });
          },
        );
      }
    }
  });

  // Additional benchmark: measure just construction time (no flush)
  describe('construction only (no flush)', () => {
    const count = 10000;
    const entries = generateEntries(count, 'small');

    bench(`putMany() construction - ${count}`, async () => {
      const dagStore = new TestStore();

      await withWrite(dagStore, async dagWrite => {
        const tree = new BTreeWrite(
          dagWrite,
          formatVersion,
          emptyHash,
          minSize,
          maxSize,
          getSizeOfEntry,
          NODE_HEADER_SIZE,
        );
        await tree.putMany(entries);
      });
    });

    bench(`sequential put() construction - ${count}`, async () => {
      const dagStore = new TestStore();

      await withWrite(dagStore, async dagWrite => {
        const tree = new BTreeWrite(
          dagWrite,
          formatVersion,
          emptyHash,
          minSize,
          maxSize,
          getSizeOfEntry,
          NODE_HEADER_SIZE,
        );
        for (const [key, value] of entries) {
          await tree.put(key, value);
        }
      });
    });
  });

  // Benchmark: updating existing entries
  describe('updating existing entries', () => {
    const count = 1000;
    const entries = generateEntries(count, 'small');
    const updates = entries.map(
      ([k, v]) => [k, `updated_${v}`] as [string, FrozenJSONValue],
    );

    bench('putMany() update all entries', async () => {
      const dagStore = new TestStore();

      await withWrite(dagStore, async dagWrite => {
        const tree = new BTreeWrite(
          dagWrite,
          formatVersion,
          emptyHash,
          minSize,
          maxSize,
          getSizeOfEntry,
          NODE_HEADER_SIZE,
        );
        // Initial load
        await tree.putMany(entries);
        // Update all entries
        await tree.putMany(updates);
        await tree.flush();
      });
    });

    bench('sequential put() update all entries', async () => {
      const dagStore = new TestStore();

      await withWrite(dagStore, async dagWrite => {
        const tree = new BTreeWrite(
          dagWrite,
          formatVersion,
          emptyHash,
          minSize,
          maxSize,
          getSizeOfEntry,
          NODE_HEADER_SIZE,
        );
        // Initial load
        await tree.putMany(entries);
        // Update all entries one by one
        for (const [key, value] of updates) {
          await tree.put(key, value);
        }
        await tree.flush();
      });
    });
  });

  // Benchmark: batch sizes
  describe('putMany batch size impact', () => {
    for (const batchSize of [10, 100, 1000, 5000]) {
      bench(`putMany() ${batchSize} entries`, async () => {
        const dagStore = new TestStore();
        const entries = generateEntries(batchSize, 'small');

        await withWrite(dagStore, async dagWrite => {
          const tree = new BTreeWrite(
            dagWrite,
            formatVersion,
            emptyHash,
            minSize,
            maxSize,
            getSizeOfEntry,
            NODE_HEADER_SIZE,
          );
          await tree.putMany(entries);
          await tree.flush();
        });
      });
    }
  });
});

describe('BTreeWrite bulk delete performance', () => {
  const formatVersion = FormatVersion.Latest;
  const minSize = 8 * 1024;
  const maxSize = 16 * 1024;

  function generateEntries(
    count: number,
    valueSize: 'small' | 'large',
  ): Array<[string, FrozenJSONValue]> {
    const entries: Array<[string, FrozenJSONValue]> = [];
    for (let i = 0; i < count; i++) {
      const key = `key${i.toString().padStart(6, '0')}`;
      const value =
        valueSize === 'small'
          ? `value${i}`
          : {
              id: i,
              name: `name${i}`,
              description: `This is a longer description for entry ${i}`,
              metadata: {
                created: Date.now(),
                tags: ['tag1', 'tag2', 'tag3'],
                nested: {
                  level1: {
                    level2: {
                      data: `nested-${i}`,
                    },
                  },
                },
              },
            };
      entries.push([key, deepFreeze(value)]);
    }
    return entries;
  }

  // Benchmark: delete all entries
  describe('deleting all entries', () => {
    const count = 1000;
    const entries = generateEntries(count, 'small');
    const allKeys = entries.map(([k]) => k);

    bench('sequential del() delete all entries', async () => {
      const dagStore = new TestStore();

      // Pre-populate tree
      const hash = await withWrite(dagStore, async dagWrite => {
        const tree = new BTreeWrite(
          dagWrite,
          formatVersion,
          emptyHash,
          minSize,
          maxSize,
          getSizeOfEntry,
          NODE_HEADER_SIZE,
        );
        await tree.putMany(entries);
        const h = await tree.flush();
        await dagWrite.setHead('main', h);
        return h;
      });

      // Delete all entries one by one
      await withWrite(dagStore, async dagWrite => {
        const tree = new BTreeWrite(
          dagWrite,
          formatVersion,
          hash,
          minSize,
          maxSize,
          getSizeOfEntry,
          NODE_HEADER_SIZE,
        );
        for (const key of allKeys) {
          await tree.del(key);
        }
        await tree.flush();
      });
    });
  });
});
