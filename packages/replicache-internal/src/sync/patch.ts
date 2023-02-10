import type {LogContext} from '@rocicorp/logger';
import type * as db from '../db/mod';
import type {PatchOperation} from '../puller';

export async function apply(
  lc: LogContext,
  dbWrite: db.Write,
  patch: PatchOperation[],
): Promise<void> {
  for (const p of patch) {
    switch (p.op) {
      case 'put': {
        await dbWrite.put(lc, p.key, p.value);
        break;
      }
      case 'del':
        await dbWrite.del(lc, p.key);
        break;

      case 'clear':
        await dbWrite.clear();
        break;
    }
  }
}
