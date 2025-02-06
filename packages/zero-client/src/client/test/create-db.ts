import {initDB} from '../../../../replicache/src/db/test-helpers.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {TestStore} from '../../../../replicache/src/dag/test-store.ts';
import {mustGetHeadHash} from '../../../../replicache/src/dag/store.ts';
import {
  type Issue,
  type IssueLabel,
  type Label,
  type Revision,
} from '../../../../zql/src/query/test/test-schemas.ts';
import {SYNC_HEAD_NAME} from '../../../../replicache/src/sync/sync-head-name.ts';
import * as FormatVersion from '../../../../replicache/src/format-version-enum.ts';
import {newWriteLocal} from '../../../../replicache/src/db/write.ts';
import type {FrozenJSONValue} from '../../../../replicache/src/frozen-json.ts';

const lc = createSilentLogContext();
export async function createDb(
  puts: Array<[string, Issue | Comment | Label | IssueLabel | Revision]>,
  timestamp: number,
) {
  const clientID = 'client-id';
  const dagStore = new TestStore();
  await initDB(
    await dagStore.write(),
    SYNC_HEAD_NAME,
    clientID,
    {},
    FormatVersion.Latest,
  );
  const dagWrite = await dagStore.write();
  const w = await newWriteLocal(
    await mustGetHeadHash(SYNC_HEAD_NAME, dagWrite),
    'mutator_name',
    JSON.stringify([]),
    null,
    dagWrite,
    timestamp,
    clientID,
    FormatVersion.Latest,
  );
  await Promise.all(
    puts.map(([key, value]) =>
      w.put(lc, key, value as unknown as FrozenJSONValue),
    ),
  );
  const syncHash = await w.commit(SYNC_HEAD_NAME);

  return {dagStore, syncHash};
}
