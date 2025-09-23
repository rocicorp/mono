import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../../zql/src/query/named.ts';
import {defineQuery} from './define-query.ts';

const schema = createSchema({
  tables: [
    table('foo')
      .columns({
        id: string(),
        val: number(),
      })
      .primaryKey('id'),
  ],
});

const builder = createBuilder(schema);

const test = defineQuery('foo', {}, ({ctx}) => {
  console.log(ctx);
  return builder.foo.where('id', '=', 'bar');
});

export const x = test({ctx: 'hi', args: undefined});

const test2 = defineQuery(
  'foo2',
  {
    validator: {
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (_data: unknown) => ({value: 1234}),
      },
    },
  },
  ({ctx, args}) => {
    console.log(ctx, args);
    return builder.foo.where('val', '=', args);
  },
);

export const x2 = test2({ctx: 'hi', args: 1234});

declare const zero: {
  run(query: typeof x | typeof x2): Promise<unknown>;
};

await zero.run(x);
await zero.run(x2);
