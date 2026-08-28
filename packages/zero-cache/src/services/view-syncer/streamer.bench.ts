// Isolates the hydration streaming path: `hydrate()` -> `Streamer.stream()` ->
// `Streamer.#streamNodes` -> `RowChange`. The `Input` is synthetic and the node
// tree is built once outside the measured loop, so the numbers reflect only the
// per-change and per-row work the Streamer itself does (change wrapping,
// generator layering, row-key and RowChange allocation) and not SQLite, joins,
// or node construction.
//
//   pnpm --filter zero-cache run bench streamer
import {bench, describe} from '../../../../shared/src/bench.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import type {Input, Output} from '../../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../../zql/src/ivm/schema.ts';
import type {Stream} from '../../../../zql/src/ivm/stream.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import {ZERO_VERSION_COLUMN_NAME} from '../replicator/schema/constants.ts';
import {hydrate} from './pipeline-driver.ts';

const TOTAL_ROWS = 5_000;

/** `parent` has one `children` relationship whose rows come from `child`. */
const CLIENT_SCHEMA: ClientSchema = {
  tables: {
    parent: {
      columns: {
        id: {type: 'number'},
        bucket: {type: 'number'},
        title: {type: 'string'},
      },
      primaryKey: ['id'],
    },
    child: {
      columns: {
        id: {type: 'number'},
        bucket: {type: 'number'},
        title: {type: 'string'},
      },
      primaryKey: ['id'],
    },
  },
};

function makeTableSpecs(): Map<string, LiteAndZqlSpec> {
  const specs = new Map<string, LiteAndZqlSpec>();
  for (const [name, {columns, primaryKey}] of Object.entries(
    CLIENT_SCHEMA.tables,
  )) {
    specs.set(name, {
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
        primaryKey: primaryKey as unknown as [string, ...string[]],
        uniqueKeys: [],
        allPotentialPrimaryKeys: [],
        minRowVersion: null,
      },
      zqlSpec: columns,
    });
  }
  return specs;
}

function makeSourceSchema(
  tableName: 'parent' | 'child',
  relationships: Record<string, SourceSchema>,
): SourceSchema {
  return {
    tableName,
    columns: CLIENT_SCHEMA.tables[tableName].columns,
    primaryKey: CLIENT_SCHEMA.tables[tableName]
      .primaryKey as unknown as SourceSchema['primaryKey'],
    relationships,
    isHidden: false,
    system: 'client',
    compareRows: (a, b) => (a.id as number) - (b.id as number),
  };
}

const CHILD_SCHEMA = makeSourceSchema('child', {});
const PARENT_SCHEMA = makeSourceSchema('parent', {children: CHILD_SCHEMA});

function makeRow(id: number): Row {
  return {
    id,
    bucket: id % 97,
    title: `row ${id}`,
    [ZERO_VERSION_COLUMN_NAME]: '01',
  };
}

/** A relationship thunk over a fixed array, as a source-backed one would be. */
function streamOf(nodes: readonly Node[]): () => Stream<Node | 'yield'> {
  return function* () {
    yield* nodes;
  };
}

/**
 * `TOTAL_ROWS` rows total, spread over `childrenPerParent + 1` rows per parent,
 * so every shape streams the same number of `RowChange`s.
 */
function makeNodes(childrenPerParent: number): Node[] {
  const parents: Node[] = [];
  let id = 0;
  while (parents.length * (childrenPerParent + 1) < TOTAL_ROWS) {
    const children: Node[] = [];
    for (let i = 0; i < childrenPerParent; i++) {
      children.push({row: makeRow(id++), relationships: {}});
    }
    parents.push({
      row: makeRow(id++),
      relationships:
        childrenPerParent === 0 ? {} : {children: streamOf(children)},
    });
  }
  return parents;
}

function makeInput(nodes: readonly Node[], schema: SourceSchema): Input {
  return {
    getSchema: () => schema,
    fetch: streamOf(nodes),
    setOutput: (_: Output) => {},
    destroy: () => {},
  };
}

describe('streamer hydration', () => {
  const tableSpecs = makeTableSpecs();

  for (const childrenPerParent of [0, 1, 4, 19]) {
    const nodes = makeNodes(childrenPerParent);
    const schema = childrenPerParent === 0 ? CHILD_SCHEMA : PARENT_SCHEMA;
    const expected = nodes.length * (childrenPerParent + 1);

    bench(
      `stream ${expected} rows, ${childrenPerParent} children per parent`,
      function* () {
        const input = makeInput(nodes, schema);
        yield () => {
          let count = 0;
          for (const change of hydrate(
            input,
            'bench-query',
            CLIENT_SCHEMA,
            tableSpecs,
          )) {
            if (change !== 'yield') {
              count++;
            }
          }
          if (count !== expected) {
            throw new Error(`Expected ${expected} row changes, got ${count}`);
          }
        };
      },
      {min_cpu_time: 1e9, min_samples: 25},
    );
  }
});
