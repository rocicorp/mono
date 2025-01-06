import {expect, test} from 'vitest';
import {nextTick} from 'vue';
import {MemorySource} from '../../zql/src/ivm/memory-source.js';
import {newQuery} from '../../zql/src/query/query-impl.js';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.js';
import {useQuery} from './use-query.js';

test('use-query', async () => {
  const tableSchema = {
    tableName: 'table',
    columns: {
      a: {type: 'number'},
      b: {type: 'string'},
    },
    primaryKey: ['a'],
    relationships: {},
  } as const;
  const ms = new MemorySource(
    tableSchema.tableName,
    tableSchema.columns,
    tableSchema.primaryKey,
  );
  ms.push({row: {a: 1, b: 'a'}, type: 'add'});
  ms.push({row: {a: 2, b: 'b'}, type: 'add'});

  const queryDelegate = new QueryDelegateImpl({table: ms});
  const tableQuery = newQuery(queryDelegate, tableSchema);

  const [data, resultType] = useQuery(() => tableQuery);
  expect(data.value).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
  ]);
  expect(resultType.value).toEqual({type: 'unknown'});

  await nextTick();

  ms.push({row: {a: 3, b: 'c'}, type: 'add'});
  expect(data.value).toEqual([
    {a: 1, b: 'a'},
    {a: 2, b: 'b'},
    {a: 3, b: 'c'},
  ]);
  expect(resultType.value).toEqual({type: 'unknown'});
});
