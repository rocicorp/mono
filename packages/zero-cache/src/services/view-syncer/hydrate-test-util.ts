import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import type {Input} from '../../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../../zql/src/ivm/schema.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';

// What `hydrate` needs to stream rows of the tables in a `ClientSchema`
// without a replica: table specs, source schemas, and an `Input` over fixed
// nodes.

/** Specs for the tables of `clientSchema`, with every column TEXT NOT NULL. */
export function tableSpecsFor(
  clientSchema: ClientSchema,
): Map<string, LiteAndZqlSpec> {
  return new Map(
    Object.entries(clientSchema.tables).map(([name, {columns, primaryKey}]) => [
      name,
      {
        tableSpec: {
          name,
          columns: Object.fromEntries(
            Object.keys(columns).map((col, pos) => [
              col,
              {
                pos,
                dataType: 'TEXT',
                characterMaximumLength: null,
                notNull: true,
              },
            ]),
          ),
          primaryKey: primaryKey as unknown as PrimaryKey,
          uniqueKeys: [],
          allPotentialPrimaryKeys: [],
          minRowVersion: null,
        },
        zqlSpec: columns,
      },
    ]),
  );
}

/** The source schema of `table` in `clientSchema`. */
export function sourceSchemaFor(
  clientSchema: ClientSchema,
  table: string,
  relationships: Record<string, SourceSchema> = {},
): SourceSchema {
  const {columns, primaryKey} = clientSchema.tables[table];
  return {
    tableName: table,
    columns,
    primaryKey: primaryKey as unknown as PrimaryKey,
    relationships,
    isHidden: false,
    system: 'client',
    // `hydrate` streams the nodes in the order the input returns them.
    compareRows: () => 0,
  };
}

/** An input whose fetch streams `nodes`, through a generator as a source does. */
export function inputOf(schema: SourceSchema, nodes: Iterable<Node>): Input {
  return {
    getSchema: () => schema,
    *fetch() {
      yield* nodes;
    },
    setOutput: () => {},
    destroy: () => {},
  };
}
