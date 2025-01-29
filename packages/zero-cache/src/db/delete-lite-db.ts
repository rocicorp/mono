import {rmSync} from 'node:fs';

export function deleteLiteDB(dbFile: string) {
  for (const suffix of ['', '-wal', '-wal2', '-shm']) {
    rmSync(`${dbFile}${suffix}`, {force: true});
  }
}
