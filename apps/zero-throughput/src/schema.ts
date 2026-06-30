import {
  createSchema,
  json,
  number,
  string,
  table,
  type Row,
} from '@rocicorp/zero';

export const eventTable = table('event')
  .from('zero_throughput_event')
  .columns({
    id: string(),
    profile: string(),
    shard: number(),
    bucket: number(),
    seq: number(),
    payload: json(),
    writtenAt: number().from('written_at'),
    updatedAt: number().from('updated_at'),
  })
  .primaryKey('id');

export const schema = createSchema({
  tables: [eventTable],
  enableLegacyQueries: true,
});

export type ThroughputEvent = Row<typeof eventTable.schema>;
