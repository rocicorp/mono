import type {Histogram} from '@opentelemetry/api';
import {cache, getMeter} from './view-syncer-instruments.ts';

const getOrCreate = cache<Histogram>();

// Custom bucket boundaries optimized for millisecond timing
const TIMING_BUCKETS = [0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000];

function getOrCreateHistogram(
  name: string,
  description: string,
  buckets?: number[],
) {
  return getOrCreate(name, name => {
    const options: {description: string; unit: string; boundaries?: number[]} =
      {
        description,
        unit: 'milliseconds',
      };

    // Use custom buckets if provided
    if (buckets) {
      options.boundaries = buckets;
    }

    return getMeter().createHistogram(name, options);
  });
}

export function wsMessageProcessingTime() {
  return getOrCreateHistogram(
    'ws-message-processing-time',
    'Time to process a websocket message. The `message.type` attribute is set in order to filter by message type.',
    TIMING_BUCKETS,
  );
}

export function replicationEventProcessingTime() {
  return getOrCreateHistogram(
    'replication-event-processing-time',
    'Time to process a replication event.',
    TIMING_BUCKETS,
  );
}

export function transactionAdvanceTime() {
  return getOrCreateHistogram(
    'cg-advance-time',
    'Time to advance all queries for a given client group after applying a new transaction to the replica.',
    TIMING_BUCKETS,
  );
}

export function changeAdvanceTime() {
  return getOrCreateHistogram(
    'change-advance-time',
    'Time to advance all queries for a given client group for in response to a single change.',
    TIMING_BUCKETS,
  );
}

export function cvrFlushTime() {
  return getOrCreateHistogram(
    'cvr-flush-time',
    'Time to flush a CVR transaction.',
    TIMING_BUCKETS,
  );
}

export function pokeTime() {
  return getOrCreateHistogram(
    'poke-flush-time',
    'Time to poke to all clients.',
    TIMING_BUCKETS,
  );
}

export function hydrationTime() {
  return getOrCreateHistogram(
    'hydration-time',
    'Time to hydrate a query.',
    TIMING_BUCKETS,
  );
}
