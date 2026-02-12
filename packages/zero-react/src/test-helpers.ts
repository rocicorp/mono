import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import {defineMutatorsWithType} from '../../zql/src/mutate/mutator-registry.ts';
import {defineMutatorWithType} from '../../zql/src/mutate/mutator.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import {
  defineQueriesWithType,
  defineQuery,
} from '../../zql/src/query/query-registry.ts';
import type {Row} from '../../zql/src/query/query.ts';

export const schema = createSchema({
  tables: [
    table('item')
      .columns({
        id: string(),
        name: string(),
      })
      .primaryKey('id'),
  ],
});

export type Item = Row<(typeof schema)['tables']['item']>;
export type StartRow = {name: string};

export const zql = createBuilder(schema);

export const queries = defineQueriesWithType<typeof schema>()({
  items: defineQuery(
    ({
      args,
    }: {
      args: {
        limit: number;
        startName: string | null;
        direction: 'forward' | 'backward';
      };
    }) => {
      const order = args.direction === 'forward' ? 'asc' : 'desc';
      const operator = args.direction === 'forward' ? '>' : '<';
      let q = zql.item.orderBy('name', order);
      if (args.startName !== null) {
        q = q.where('name', operator, args.startName);
      }
      return q.limit(args.limit);
    },
  ),
  item: defineQuery(({args}: {args: {id: string}}) =>
    zql.item.where('id', args.id).one(),
  ),
});

export const mutators = defineMutatorsWithType<typeof schema>()({
  populateItems: defineMutatorWithType<typeof schema>()<{count: number}>(
    async ({tx, args}) => {
      for (let i = 0; i < args.count; i++) {
        const id = `${i + 1}`;
        const paddedNum = String(i + 1).padStart(4, '0');
        await tx.mutate.item.insert({
          id,
          name: `Item ${paddedNum}`,
        });
      }
    },
  ),
  addItem: defineMutatorWithType<typeof schema>()<{
    id: string;
    name: string;
  }>(async ({tx, args}) => {
    await tx.mutate.item.insert(args);
  }),
});

// Query helper functions
export const getPageQuery = (
  limit: number,
  start: StartRow | null,
  dir: 'forward' | 'backward',
) =>
  queries.items({
    limit,
    startName: start?.name ?? null,
    direction: dir,
  });

export const getSingleQuery = (id: string) => queries.item({id});

export const toStartRow = (row: Item): StartRow => ({name: row.name});
