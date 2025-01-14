/* eslint-disable @typescript-eslint/naming-convention */
import {beforeEach, describe, expect, test} from 'vitest';
import type {Schema} from '../../../zero-schema/src/schema.js';
import {createQueryDelegate} from '../test/source-factory.js';
import {
  newQuery,
  type QueryDelegate,
} from '../../../zql/src/query/query-impl.js';
import {must} from '../../../shared/src/must.js';

describe('discord not-exists user report', () => {
  let queryDelegate: QueryDelegate;
  const TYL_trackableGroup = {
    tableName: 'TYL_trackableGroup',
    columns: {
      trackableId: 'string',
      group: 'string',
      user_id: 'string',
    },
    primaryKey: ['trackableId', 'group'],
  } as const;
  const TYL_trackable = {
    tableName: 'TYL_trackable',
    columns: {
      id: 'string',
      name: 'string',
    },
    relationships: {
      trackableGroup: {
        sourceField: ['id'],
        destField: ['trackableId'],
        destSchema: TYL_trackableGroup,
      },
    },
    primaryKey: ['id'],
  } as const;

  const schema = {
    version: 1,
    tables: {
      TYL_trackableGroup,
      TYL_trackable,
    },
  } satisfies Schema;
  beforeEach(() => {
    queryDelegate = createQueryDelegate(schema);

    const trackableSource = must(queryDelegate.getSource('TYL_trackable'));
    trackableSource.push({
      type: 'add',
      row: {
        id: '001',
        name: 'trackable 1',
      },
    });
  });

  test('not exists', () => {
    const query = newQuery(queryDelegate, schema.tables.TYL_trackable)
      .where(({not, exists}) =>
        not(exists('trackableGroup', q => q.where('group', '=', 'archived'))),
      )
      .related('trackableGroup');

    const view = query.materialize();

    // trackable is there
    expect(view.data).toMatchInlineSnapshot(`
      [
        {
          "id": "001",
          "name": "trackable 1",
          "trackableGroup": [],
        },
      ]
    `);

    const trackableGroupSource = must(
      queryDelegate.getSource('TYL_trackableGroup'),
    );
    trackableGroupSource.push({
      type: 'add',
      row: {
        trackableId: '001',
        group: 'archived',
        user_id: '001',
      },
    });

    // trackable removed due to `not exists trackableGroup where group = 'archived'`
    expect(view.data).toMatchInlineSnapshot(`[]`);

    trackableGroupSource.push({
      type: 'remove',
      row: {
        trackableId: '001',
        group: 'archived',
        user_id: '001',
      },
    });

    // trackable back since we deleted the archived group
    expect(view.data).toMatchInlineSnapshot(`
      [
        {
          "id": "001",
          "name": "trackable 1",
          "trackableGroup": [],
        },
      ]
    `);
  });
});
