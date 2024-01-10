import type {LogContext} from '@rocicorp/logger';
import type {Write} from '../db/write.js';
import {deepFreeze} from '../frozen-json.js';
import type {PatchOperation} from '../patch-operation.js';

export async function apply(
  lc: LogContext,
  dbWrite: Write,
  patch: readonly PatchOperation[],
): Promise<void> {
  for (const p of patch) {
    switch (p.op) {
      case 'put': {
        await dbWrite.put(lc, p.key, deepFreeze(p.value));
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
