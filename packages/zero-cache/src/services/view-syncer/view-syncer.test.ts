import {describe, expect, test} from 'vitest';
import {DurableStorage} from '../../storage/durable-storage.js';
import {runWithDurableObjectStorage} from '../../test/do.js';
import {createSilentLogContext} from '../../test/logger.js';
import type {InvalidationWatcherRegistry} from '../invalidation-watcher/registry.js';
import {ViewSyncerService} from './view-syncer.js';

describe('view-syncer/service', () => {
  const serviceID = '9876';

  test('initializes schema', async () => {
    await runWithDurableObjectStorage(async storage => {
      const vs = new ViewSyncerService(
        createSilentLogContext(),
        serviceID,
        new DurableStorage(storage),
        {} as InvalidationWatcherRegistry,
      );

      await vs.run();

      expect(await storage.get('/vs/9876/storage_schema_meta')).toEqual({
        // Update versions as necessary
        version: 1,
        maxVersion: 1,
        minSafeRollbackVersion: 1,
      });
    });
  });
});
