/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
/* eslint-disable no-console */
import sqlite3 from '@rocicorp/zero-sqlite3';
import fs from 'fs';
import path from 'path';
import {SQLiteDatabaseManager} from './sqlite-store.ts';

export const getTestSQLiteDatabaseManager = (logging: boolean = false) =>
  new SQLiteDatabaseManager({
    open: name => {
      const filename = path.resolve(__dirname, `${name}.db`);
      // this cannot be :memory: because multiple read connections must access
      // the same database
      const db = sqlite3(filename);
      return {
        close: () => {
          db.close();
        },
        destroy: () => {
          db.close();
          if (fs.existsSync(filename)) {
            fs.unlinkSync(filename);
          }
        },
        prepare: (sql: string) => {
          const stmt = db.prepare(sql);
          if (logging) {
            console.log('prepare', sql);
          }
          return {
            all: <T>(...params: unknown[]): T[] => {
              const result = params.length ? stmt.all(...params) : stmt.all();
              if (logging) {
                console.log('all', sql, params, result);
              }
              return result as T[];
            },
            run: (...params: unknown[]): void => {
              if (logging) {
                console.log('run', sql, params);
              }
              if (params.length) {
                stmt.run(...params);
              } else {
                stmt.run();
              }
            },
            finalize: () => {
              // no-op
            },
          };
        },
      };
    },
  });
