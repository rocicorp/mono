import {metrics, type Meter} from '@opentelemetry/api';

let meter: Meter | undefined;
function getMeter() {
  if (!meter) {
    meter = metrics.getMeter('view-syncer');
  }
  return meter;
}

// intentional lazy initialization so it is not started before the SDK is started.
export default {
  get counters() {
    return {
      replicationEvents: getMeter().createCounter('replication-events', {
        description: 'Number of replication events processed',
      }),
      crudMutations: getMeter().createCounter('crud-mutations', {
        description: 'Number of CRUD mutations processed',
      }),
      customMutations: getMeter().createCounter('custom-mutations', {
        description: 'Number of custom mutations processed',
      }),
      pushes: getMeter().createCounter('pushes', {
        description: 'Number of pushes processed by the pusher',
      }),
      queryHydrations: getMeter().createCounter('query-hydrations', {
        description: 'Number of query hydrations',
      }),
      cvrRowsFlushed: getMeter().createCounter('cvr-rows-flushed', {
        description: 'Number of rows flushed to all CVRs',
      }),
      rowsPoked: getMeter().createCounter('rows-poked', {
        description: 'Number of rows poked',
      }),
      pokeTransactions: getMeter().createCounter('poke-transactions', {
        description: 'Number of poke transactions (pokeStart,pokeEnd) pairs',
      }),
    };
  },

  get upDownCounters() {
    return {
      activeConnections: getMeter().createUpDownCounter('active-connections', {
        description: 'Number of active websocket connections',
      }),
      activeQueries: getMeter().createUpDownCounter('active-queries', {
        description: 'Number of active queries',
      }),
      activeClients: getMeter().createUpDownCounter('active-clients', {
        description: 'Number of active clients',
      }),
      activeClientGroups: getMeter().createUpDownCounter(
        'active-client-groups',
        {
          description: 'Number of active client groups',
        },
      ),
      activeViewSyncerInstances: getMeter().createUpDownCounter(
        'active-view-syncer-instances',
        {
          description: 'Number of active view syncer instances',
        },
      ),
      activePusherInstances: getMeter().createUpDownCounter(
        'active-pusher-instances',
        {
          description: 'Number of active pusher instances',
        },
      ),
      activeIvmStorageInstances: getMeter().createUpDownCounter(
        'active-ivm-storage-instances',
        {
          description: 'Number of active ivm operator storage instances',
        },
      ),
    };
  },

  get histograms() {
    return {
      wsMessageProcessingTime: getMeter().createHistogram(
        'ws-message-processing-time',
        {
          description:
            'Time to process a websocket message. The `message.type` attribute is set in order to filter by message type.',
          unit: 'milliseconds',
        },
      ),
      replicationEventProcessingTime: getMeter().createHistogram(
        'replication-event-processing-time',
        {
          description: 'Time to process a replication event.',
          unit: 'milliseconds',
        },
      ),
      transactionAdvanceTime: getMeter().createHistogram('cg-advance-time', {
        description:
          'Time to advance all queries for a given client group after applying a new transaction to the replica.',
        unit: 'milliseconds',
      }),
      changeAdvanceTime: getMeter().createHistogram('change-advance-time', {
        description:
          'Time to advance all queries for a given client group for in response to a single change.',
        unit: 'milliseconds',
      }),
      cvrFlushTime: getMeter().createHistogram('cvr-flush-time', {
        description: 'Time to flush a CVR transaction.',
        unit: 'milliseconds',
      }),
      pokeTime: getMeter().createHistogram('poke-flush-time', {
        description: 'Time to poke to all clients.',
        unit: 'milliseconds',
      }),
      hydrationTime: getMeter().createHistogram('hydration-time', {
        description: 'Time to hydrate a query.',
        unit: 'milliseconds',
      }),
    };
  },
};
