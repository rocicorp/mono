import {jsonObjectSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {zeroEventSchema} from './index.ts';

export const statusSchema = v.literalUnion('OK', 'ERROR');

export type Status = v.Infer<typeof statusSchema>;

/**
 * A StatusEvent conveys the most current status of a given component,
 * with each event replacing any preceding status events (based on the
 * `time` field) for the same component.
 *
 * All StatusEvents have a `type` value that starts with `zero/events/status/`,
 * with common fields (e.g. `stage`, `description`, `errorDetails`) that can
 * be used to describe the state of any component even if the specific subtype
 * is not known. In this respect, an event consumer can subscribe to
 * "zero/events/status/*" and display general status information without
 * needing to understand subtype-specific fields.
 */
export const statusEventSchema = zeroEventSchema.extend({
  /**
   * The component of the zero-cache to which the event pertains,
   * e.g. "replication".
   */
  component: v.string(),

  /** Whether the component is healthy. */
  status: statusSchema,

  /**
   * The stage describing the component's current state. This is meant to be
   * both machine and human readable (e.g. a single work serving as a well-known
   * constant).
   */
  stage: v.string(),

  /**
   * An optional, human readable description.
   */
  description: v.string().optional(),

  /** Error details should be supplied for an 'ERROR' status message. */
  errorDetails: jsonObjectSchema.optional(),
});

export type StatusEvent<
  T extends v.Infer<typeof statusEventSchema> & {
    type: `zero/events/status/${string}`;
  },
> = T;

const replicatedColumnSchema = v.object({
  column: v.string(),
  upstreamType: v.string(),
  clientType: v.string().nullable(),
});

const replicatedTableSchema = v.object({
  table: v.string(),
  columns: v.array(replicatedColumnSchema),
});

export type ReplicatedTable = v.Infer<typeof replicatedTableSchema>;

const indexedColumnSchema = v.object({
  column: v.string(),
  dir: v.literalUnion('ASC', 'DESC'),
});

const replicatedIndexSchema = v.object({
  table: v.string(),
  columns: v.array(indexedColumnSchema),
  unique: v.boolean(),
});

export type ReplicatedIndex = v.Infer<typeof replicatedIndexSchema>;

const replicationStageSchema = v.literalUnion(
  'Initializing',
  'Indexing',
  'Replicating',
);

export type ReplicationStage = v.Infer<typeof replicationStageSchema>;

/**
 * A ReplicationStatusEvent is a StatusEvent event subtype for the
 * "replication" component.
 */
export const replicationStatusEventSchema = statusEventSchema.extend({
  type: v.literal('zero/events/status/replication/v1'),
  component: v.literal('replication'),
  stage: replicationStageSchema,

  tables: v.array(replicatedTableSchema),
  indexes: v.array(replicatedIndexSchema),
  replicaSize: v.number().optional(),
});

// CloudEvent type: "zero.status/replication/v1"
export type ReplicationStatusEvent = StatusEvent<
  v.Infer<typeof replicationStatusEventSchema>
>;
