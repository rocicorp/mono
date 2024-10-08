import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {beforeEach, describe, expect, test} from 'vitest';
import {testDBs} from '../../test/db.js';
import type {PostgresDB} from '../../types/pg.js';
import {CVRStore} from './cvr-store.js';
import type {CVRSnapshot} from './cvr.js';
import {type RowsRow, setupCVRTables} from './schema/cvr.js';
import type {CVRVersion} from './schema/types.js';

describe('view-syncer/cvr-store', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;
  let store: CVRStore;

  const CVR_ID = 'my-cvr';

  beforeEach(async () => {
    db = await testDBs.create('view_syncer_cvr_schema');
    await db.begin(tx => setupCVRTables(lc, tx));
    await db.unsafe(`
    INSERT INTO cvr.instances("clientGroupID", version, "lastActive")
      VALUES('${CVR_ID}', '01', '2024-09-04');
    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"1"}', '01', '01', NULL);
    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"2"}', '01', '01', '{"foo":1}');
    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"3"}', '01', '01', '{"bar":2}');
    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"4"}', '01', '01', '{"foo":2,"bar":3}');

    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"5"}', '01', '02', NULL);
    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"6"}', '01', '02', '{"foo":1}');
    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"7"}', '01', '02', '{"bar":2}');
    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"8"}', '01', '02', '{"foo":2,"bar":3}');

    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"9"}', '01', '03', NULL);
    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"10"}', '01', '03', '{"foo":1}');
    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"11"}', '01', '03', '{"bar":2}');
    INSERT INTO cvr.rows ("clientGroupID", "schema", "table", "rowKey", "rowVersion", "patchVersion", "refCounts")
      VALUES('${CVR_ID}', '', 'issues', '{"id":"12"}', '01', '03', '{"foo":2,"bar":3}');
      `);
    store = new CVRStore(lc, db, CVR_ID);
  });

  async function catchupRows(
    after: CVRVersion,
    upTo: CVRVersion,
    excludeQueryHashes: string[] = [],
  ): Promise<RowsRow[]> {
    const rows = [];
    for await (const batch of store.catchupRowPatches(
      lc,
      after,
      {
        version: upTo,
      } as CVRSnapshot,
      excludeQueryHashes,
    )) {
      rows.push(...batch);
    }
    return rows;
  }

  test('catchupRows', async () => {
    // After 01, up to 02:
    expect(await catchupRows({stateVersion: '01'}, {stateVersion: '02'}))
      .toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": null,
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": {
            "foo": 1,
          },
          "rowKey": {
            "id": "6",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": {
            "bar": 2,
          },
          "rowKey": {
            "id": "7",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": {
            "bar": 3,
            "foo": 2,
          },
          "rowKey": {
            "id": "8",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);

    // After 00, up to 02, excluding query hash 'bar'
    expect(
      await catchupRows({stateVersion: '00'}, {stateVersion: '02'}, ['bar']),
    ).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "01",
          "refCounts": null,
          "rowKey": {
            "id": "1",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "01",
          "refCounts": {
            "foo": 1,
          },
          "rowKey": {
            "id": "2",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": null,
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": {
            "foo": 1,
          },
          "rowKey": {
            "id": "6",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);

    // After 01, up to 03, excluding multiple query hashes 'foo' and 'bar'
    expect(
      await catchupRows({stateVersion: '01'}, {stateVersion: '03'}, [
        'foo',
        'bar',
      ]),
    ).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "02",
          "refCounts": null,
          "rowKey": {
            "id": "5",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
        {
          "clientGroupID": "my-cvr",
          "patchVersion": "03",
          "refCounts": null,
          "rowKey": {
            "id": "9",
          },
          "rowVersion": "01",
          "schema": "",
          "table": "issues",
        },
      ]
    `);
  });
});
