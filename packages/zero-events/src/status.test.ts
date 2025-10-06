import type {ReplicationStatusEvent} from './status.ts';

export const replicationStatusEventIsValidType: ReplicationStatusEvent = {
  type: 'zero/events/status/replication/v1',
  time: '2025-10-06T23:25:09.421Z',
  status: 'OK',
  component: 'replication',
  stage: 'Indexing',
  state: {tables: [], indexes: []},
};
