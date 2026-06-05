import type {IndexRequirement} from '../../zero-protocol/src/inspect-up.ts';
import type {Relationship, Schema} from '../../zero-types/src/schema.ts';
import {clientToServer} from './name-mapper.ts';

/**
 * Walks a schema's relationships and returns the set of join field sets that
 * need to be backed by an index, in *both* directions of every relationship.
 *
 * Why both directions? Zero maintains `related()`/`whereExists()` queries
 * incrementally and reacts to changes from either table. For a relationship
 * `A.sourceField -> B.destField`:
 * - fetching related `B` rows for an `A` row looks `B` up by `destField`, and
 * - reacting to a changed `B` row looks the matching `A` rows up by `sourceField`.
 *
 * So an index is needed on `B.destField` *and* on `A.sourceField`. Typically
 * one side is the primary key (already indexed) and the other is a foreign
 * key that needs an explicit index. Junction (many-to-many) relationships have
 * two hops, and every field on both hops is emitted.
 *
 * The returned requirements use server names (resolved via the schema's
 * client->server mapping), so they can be checked directly against the
 * database's indexes. This runs purely on the schema — no database access —
 * so it is safe to call on the client (e.g. from the inspector).
 */
export function enumerateRelationshipIndexRequirements(
  schema: Schema,
): IndexRequirement[] {
  const clientToServerNames = clientToServer(schema.tables);
  const requirements: IndexRequirement[] = [];

  const add = (req: {
    ownerTable: string;
    relationship: string;
    hop: number;
    hopCount: number;
    side: 'source' | 'dest';
    cardinality: 'one' | 'many';
    clientTable: string;
    clientColumns: readonly string[];
  }) => {
    const serverTable = clientToServerNames.tableNameIfKnown(req.clientTable);
    if (serverTable === undefined) {
      // References a table not in `schema.tables`. `createSchema()` validates
      // against this, so it shouldn't happen, but guard rather than throw.
      return;
    }
    requirements.push({
      ownerTable: req.ownerTable,
      relationship: req.relationship,
      hop: req.hop,
      hopCount: req.hopCount,
      side: req.side,
      cardinality: req.cardinality,
      clientTable: req.clientTable,
      clientColumns: [...req.clientColumns],
      serverTable,
      serverColumns: clientToServerNames.columns(req.clientTable, [
        ...req.clientColumns,
      ]),
    });
  };

  for (const [ownerTable, relationships] of Object.entries(
    schema.relationships,
  )) {
    for (const [relationship, connections] of Object.entries(relationships)) {
      const conns = connections as Relationship;
      let sourceTable = ownerTable;
      for (let hop = 0; hop < conns.length; hop++) {
        const conn = conns[hop];
        const common = {
          ownerTable,
          relationship,
          hop: hop + 1,
          hopCount: conns.length,
          cardinality: conn.cardinality,
        };
        add({
          ...common,
          side: 'source',
          clientTable: sourceTable,
          clientColumns: conn.sourceField,
        });
        add({
          ...common,
          side: 'dest',
          clientTable: conn.destSchema,
          clientColumns: conn.destField,
        });
        sourceTable = conn.destSchema;
      }
    }
  }

  return requirements;
}
