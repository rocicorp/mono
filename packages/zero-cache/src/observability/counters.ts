import type {Counter} from '@opentelemetry/api';
import {cache, getMeter} from './view-syncer-instruments.ts';

const getOrCreateCounter = cache<Counter>();

function createCounter(name: string, options: {description: string}) {
  return getMeter().createCounter(name, {
    description: options.description,
  });
}

export function replicationEvents() {
  return getOrCreateCounter('replication-events', name =>
    createCounter(name, {
      description: 'Number of replication events processed',
    }),
  );
}

export function crudMutations() {
  return getOrCreateCounter('crud-mutations', name =>
    createCounter(name, {
      description: 'Number of CRUD mutations processed',
    }),
  );
}

export function customMutations() {
  return getOrCreateCounter('custom-mutations', name =>
    createCounter(name, {
      description: 'Number of custom mutations processed',
    }),
  );
}

export function pushes() {
  return getOrCreateCounter('pushes', name =>
    createCounter(name, {
      description: 'Number of pushes processed by the pusher',
    }),
  );
}

export function queryHydrations() {
  return getOrCreateCounter('query-hydrations', name =>
    createCounter(name, {
      description: 'Number of query hydrations',
    }),
  );
}

export function cvrRowsFlushed() {
  return getOrCreateCounter('cvr-rows-flushed', name =>
    createCounter(name, {
      description: 'Number of rows flushed to all CVRs',
    }),
  );
}

export function rowsPoked() {
  return getOrCreateCounter('rows-poked', name =>
    createCounter(name, {
      description: 'Number of rows poked',
    }),
  );
}

export function pokeTransactions() {
  return getOrCreateCounter('poke-transactions', name =>
    createCounter(name, {
      description: 'Number of poke transactions (pokeStart,pokeEnd) pairs',
    }),
  );
}
