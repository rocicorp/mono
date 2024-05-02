import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import type {Mutation} from 'zero-protocol/src/push.js';
import {PostgresDB, postgresTypeConfig} from '../../types/pg.js';
import type {Service} from '../service.js';
import {processMutation} from './mutagen.js';

export class MutagenService implements Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #db: PostgresDB;

  constructor(lc: LogContext, clientGroupID: string, dbUri: string) {
    this.#lc = lc;
    this.id = clientGroupID;
    this.#db = postgres(dbUri, {
      ...postgresTypeConfig(),
    });
  }

  async processMutations(mutations: Mutation[]) {
    for (const mutation of mutations) {
      // intentionally serial
      await processMutation(this.#lc, this.#db, mutation);
    }
  }

  run(): Promise<void> {
    return Promise.resolve();
  }

  stop(): Promise<void> {
    return Promise.resolve();
  }
}
