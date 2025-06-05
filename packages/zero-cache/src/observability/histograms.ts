import type {Histogram} from '@opentelemetry/api';
import {cache, getMeter} from './view-syncer-instruments.ts';

const getOrCreateHistogram = cache<Histogram>();
function createHistogram(
  name: string,
  options: {description: string; unit?: string},
) {
  return getMeter().createHistogram(name, options);
}

export function wsMessageProcessingTime() {
  return getOrCreateHistogram('ws-message-processing-time', name =>
    createHistogram(name, {
      description:
        'Time to process a websocket message. The `message.type` attribute is set in order to filter by message type.',
      unit: 'milliseconds',
    }),
  );
}

export function replicationEventProcessingTime() {
  return getOrCreateHistogram('replication-event-processing-time', name =>
    createHistogram(name, {
      description: 'Time to process a replication event.',
      unit: 'milliseconds',
    }),
  );
}

export function transactionAdvanceTime() {
  return getOrCreateHistogram('cg-advance-time', name =>
    createHistogram(name, {
      description:
        'Time to advance all queries for a given client group after applying a new transaction to the replica.',
      unit: 'milliseconds',
    }),
  );
}

export function changeAdvanceTime() {
  return getOrCreateHistogram('change-advance-time', name =>
    createHistogram(name, {
      description:
        'Time to advance all queries for a given client group for in response to a single change.',
      unit: 'milliseconds',
    }),
  );
}

export function cvrFlushTime() {
  return getOrCreateHistogram('cvr-flush-time', name =>
    createHistogram(name, {
      description: 'Time to flush a CVR transaction.',
      unit: 'milliseconds',
    }),
  );
}

export function pokeTime() {
  return getOrCreateHistogram('poke-flush-time', name =>
    createHistogram(name, {
      description: 'Time to poke to all clients.',
      unit: 'milliseconds',
    }),
  );
}

export function hydrationTime() {
  return getOrCreateHistogram('hydration-time', name =>
    createHistogram(name, {
      description: 'Time to hydrate a query.',
      unit: 'milliseconds',
    }),
  );
}
