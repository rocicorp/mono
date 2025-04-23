import {metrics} from '@opentelemetry/api';

const meter = metrics.getMeter('view-syncer');

export const counters = {
  replicationEvents: meter.createCounter('replication-events', {
    description: 'Number of replication events processed',
  }),
  crudMutations: meter.createCounter('crud-mutations', {
    description: 'Number of CRUD mutations processed',
  }),
  customMutations: meter.createCounter('custom-mutations', {
    description: 'Number of custom mutations processed',
  }),
  pushes: meter.createCounter('pushes', {
    description: 'Number of pushes processed by the pusher',
  }),
  queryHydrations: meter.createCounter('query-hydrations', {
    description: 'Number of query hydrations',
  }),
  cvrRowsFlushed: meter.createCounter('cvr-rows-flushed', {
    description: 'Number of rows flushed to all CVRs',
  }),
  rowsPoked: meter.createCounter('rows-poked', {
    description: 'Number of rows poked',
  }),
  pokeTransactions: meter.createCounter('poke-transactions', {
    description: 'Number of poke transactions (pokeStart,pokeEnd) pairs',
  }),
  ivmRowsProcessed: meter.createCounter('ivm-changes-processed', {
    description: 'Number of rows emitted by IVM pipelines',
  }),
};

export const upDownCounters = {
  activeConnections: meter.createUpDownCounter('active-connections', {
    description: 'Number of active websocket connections',
  }),
  activeQueries: meter.createUpDownCounter('active-queries', {
    description: 'Number of active queries',
  }),
  activeClients: meter.createUpDownCounter('active-clients', {
    description: 'Number of active clients',
  }),
  activeClientGroups: meter.createUpDownCounter('active-client-groups', {
    description: 'Number of active client groups',
  }),
  activeViewSyncerInstances: meter.createUpDownCounter(
    'active-view-syncer-instances',
    {
      description: 'Number of active view syncer instances',
    },
  ),
  activePusherInstances: meter.createUpDownCounter('active-pusher-instances', {
    description: 'Number of active pusher instances',
  }),
  activeIvmStorageInstances: meter.createUpDownCounter(
    'active-ivm-storage-instances',
    {
      description: 'Number of active ivm operator storage instances',
    },
  ),
};

export const histograms = {
  wsMessageProcessingTime: meter.createHistogram('ws-message-processing-time', {
    description:
      'Time to process a websocket message. The `message.type` attribute is set in order to filter by message type.',
    unit: 'milliseconds',
  }),
  replicationEventProcessingTime: meter.createHistogram(
    'replication-event-processing-time',
    {
      description: 'Time to process a replication event.',
      unit: 'milliseconds',
    },
  ),
  pokeFlushTime: meter.createHistogram('poke-flush-time', {
    description: 'Time to flush a poke transaction.',
    unit: 'milliseconds',
  }),
  hydrationTime: meter.createHistogram('hydration-time', {
    description: 'Time to hydrate a query.',
    unit: 'milliseconds',
  }),
};
