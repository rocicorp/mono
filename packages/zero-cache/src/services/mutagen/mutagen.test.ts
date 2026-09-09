import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import * as MutationType from '../../../../zero-protocol/src/mutation-type-enum.ts';
import type {Mutation} from '../../../../zero-protocol/src/mutation.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {WriteAuthorizerImpl} from '../../auth/write-authorizer.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {CREATE_TABLE_METADATA_TABLE} from '../replicator/schema/table-metadata.ts';
import {MutagenService} from './mutagen.ts';

describe('mutagen/MutagenService', () => {
  const lc = createSilentLogContext();
  let tempDir: string;
  let replicaFile: string;
  let storageDb: Database;
  let writeAuthzStorage: DatabaseStorage;
  let closeSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'zero-cache-mutagen'));
    replicaFile = path.join(tempDir, 'replica.db');
    const replica = new Database(lc, replicaFile);
    replica.exec(/*sql*/ `
      CREATE TABLE "test-app.permissions" (permissions, hash);
      INSERT INTO "test-app.permissions" (permissions, hash) VALUES (null, 'h');
    `);
    replica.exec(CREATE_TABLE_METADATA_TABLE);
    replica.close();

    storageDb = new Database(lc, ':memory:');
    storageDb.prepare(CREATE_STORAGE_TABLE).run();
    writeAuthzStorage = new DatabaseStorage(storageDb);

    closeSpy = vi.spyOn(Database.prototype, 'close');
  });

  afterEach(async () => {
    closeSpy.mockRestore();
    storageDb.close();
    await fs.rm(tempDir, {recursive: true, force: true});
  });

  function createService(upstream: PostgresDB) {
    return new MutagenService(
      lc,
      {appID: 'test-app', shardNum: 0},
      'cg-1',
      upstream,
      {
        replica: {file: replicaFile},
        perUserMutationLimit: {},
      } as ZeroConfig,
      writeAuthzStorage,
    );
  }

  const mutation: Mutation = {
    type: MutationType.CRUD,
    id: 1,
    clientID: 'c1',
    name: '_zero_crud',
    args: [{ops: []}],
    timestamp: 0,
  };

  test('stop() closes the replica handle', async () => {
    const service = createService({} as PostgresDB);
    service.ref();
    expect(closeSpy).not.toHaveBeenCalled();

    service.unref();
    await service.run();

    expect(closeSpy).toHaveBeenCalledTimes(1);
  });

  test('replica stays open while a push holds a ref', async () => {
    // A fake upstream whose transaction hangs until released, modeling a
    // mutation that is being processed when its connection closes.
    const tx = resolver<void>();
    const upstream = {
      begin: () => tx.promise,
    } as unknown as PostgresDB;

    const service = createService(upstream);
    service.ref(); // the connection
    service.ref(); // the push (see SyncerWsMessageHandler)
    const result = service.processMutation(mutation, undefined);

    // The connection goes away while the push is in flight.
    service.unref();
    expect(service.hasRefs()).toBe(true);
    expect(closeSpy).not.toHaveBeenCalled();

    tx.resolve();
    expect(await result).toBeUndefined();
    service.unref(); // the push completes
    await service.run();
    expect(closeSpy).toHaveBeenCalledTimes(1);
  });

  test('replica is closed even if destroying the authorizer storage fails', async () => {
    const service = createService({} as PostgresDB);
    const destroySpy = vi
      .spyOn(WriteAuthorizerImpl.prototype, 'destroy')
      .mockImplementation(() => {
        throw new Error('boom');
      });
    try {
      service.ref();
      service.unref();
      await service.run();
      expect(destroySpy).toHaveBeenCalledTimes(1);
      expect(closeSpy).toHaveBeenCalledTimes(1);
    } finally {
      destroySpy.mockRestore();
    }
  });
});
