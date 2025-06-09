import type {Counter} from '@opentelemetry/api';
import {cache, getMeter} from './view-syncer-instruments.ts';

const getOrCreate = cache<Counter>();

function getOrCreateCounter(name: string, description: string) {
  return getOrCreate(name, name =>
    getMeter().createCounter(name, {description}),
  );
}

export function replicationEvents() {
  return getOrCreateCounter(
    'replication-events',
    'Number of replication events processed',
  );
}

export function crudMutations() {
  return getOrCreateCounter(
    'crud-mutations',
    'Number of CRUD mutations processed',
  );
}

export function customMutations() {
  return getOrCreateCounter(
    'custom-mutations',
    'Number of custom mutations processed',
  );
}

export function pushes() {
  return getOrCreateCounter(
    'pushes',
    'Number of pushes processed by the pusher',
  );
}

export function queryHydrations() {
  return getOrCreateCounter('query-hydrations', 'Number of query hydrations');
}

export function cvrRowsFlushed() {
  return getOrCreateCounter(
    'cvr-rows-flushed',
    'Number of rows flushed to all CVRs',
  );
}

export function rowsPoked() {
  return getOrCreateCounter('rows-poked', 'Number of rows poked');
}

export function pokeTransactions() {
  return getOrCreateCounter(
    'poke-transactions',
    'Number of poke transactions (pokeStart,pokeEnd) pairs',
  );
}
