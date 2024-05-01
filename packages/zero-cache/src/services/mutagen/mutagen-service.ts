import type {LogContext} from '@rocicorp/logger';
import type {Mutation} from 'zero-protocol/src/push.js';
import type {PostgresDB} from '../../types/pg.js';
import type {Service} from '../service.js';
import {processMutation} from './mutagen.js';

export class MutagenService implements Service {
  readonly id: string;
  readonly #lc: LogContext;
  readonly #db: PostgresDB;

  constructor(lc: LogContext, clientGroupID: string, db: PostgresDB) {
    this.#lc = lc;
    this.id = clientGroupID;
    this.#db = db;
  }

  processMutation(mutation: Mutation) {
    return processMutation(this.#lc, this.#db, mutation);
  }

  run(): Promise<void> {
    return Promise.resolve();
  }

  stop(): Promise<void> {
    return Promise.resolve();
  }
}
