/**
 * Building blocks for the client scenarios: server messages in the shapes
 * zero-cache sends, with server table and column names.
 */
import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import {hashOfNameAndArgs} from '../../../../zero-protocol/src/query-hash.ts';
import type {Step} from '../harness.ts';

type JSONObject = {readonly [key: string]: ReadonlyJSONValue};

/** The hash a named query is registered under, as the client computes it. */
export function queryHash(name: string, args?: ReadonlyJSONValue): string {
  return hashOfNameAndArgs(name, args === undefined ? [] : [args]);
}

/**
 * Lets the client finish starting up and open its first socket. Zero opens it
 * asynchronously after construction, so a scenario whose first step talks to
 * the server starts with this.
 */
export const start: Step = {op: 'advance', ms: 0};

export const connected: Step = {
  op: 'receive',
  message: ['connected', {wsid: 'w1'}],
};

export const pong: Step = {op: 'receive', message: ['pong', {}]};

export type PokePart = {
  lastMutationIDChanges?: Record<string, number>;
  desiredQueriesPatches?: Record<string, ReadonlyJSONValue[]>;
  gotQueriesPatch?: ReadonlyJSONValue[];
  rowsPatch?: ReadonlyJSONValue[];
  mutationsPatch?: ReadonlyJSONValue[];
};

/** The three frames of a poke with one text part. */
export function pokeMessages(
  pokeID: string,
  baseCookie: string | null,
  cookie: string,
  part: PokePart,
): ReadonlyJSONValue[] {
  return [
    ['pokeStart', {pokeID, baseCookie}],
    ['pokePart', {pokeID, ...part}],
    ['pokeEnd', {pokeID, cookie}],
  ];
}

/** A poke with one text part, one frame per step. */
export function poke(
  pokeID: string,
  baseCookie: string | null,
  cookie: string,
  part: PokePart,
): Step[] {
  return pokeMessages(pokeID, baseCookie, cookie, part).map(message => ({
    op: 'receive',
    message,
  }));
}

/** An `issues` row as the server sends it. */
export function issue(
  id: string,
  fields: {
    title?: string;
    description?: string;
    closed?: boolean;
    ownerId?: string | null;
    createdAt?: number;
  } = {},
): JSONObject {
  return {
    id,
    title: fields.title ?? id.toUpperCase(),
    description: fields.description ?? '',
    closed: fields.closed ?? false,
    owner_id: fields.ownerId ?? null,
    createdAt: fields.createdAt ?? 1,
  };
}

/** A `comments` row as the server sends it. */
export function comment(
  id: string,
  issueID: string,
  fields: {text?: string; authorId?: string; createdAt?: number} = {},
): JSONObject {
  return {
    id,
    authorId: fields.authorId ?? 'u1',
    issue_id: issueID,
    text: fields.text ?? '',
    createdAt: fields.createdAt ?? 1,
  };
}

/** A `users` row as the server sends it. */
export function user(
  id: string,
  name: string,
  metadata: ReadonlyJSONValue = null,
): JSONObject {
  return {id, name, metadata};
}

export function put(tableName: string, value: JSONObject): ReadonlyJSONValue {
  return {op: 'put', tableName, value};
}

export function del(tableName: string, id: JSONObject): ReadonlyJSONValue {
  return {op: 'del', tableName, id};
}

export const gotPut = (hash: string): ReadonlyJSONValue => ({op: 'put', hash});
export const gotDel = (hash: string): ReadonlyJSONValue => ({op: 'del', hash});

/** Marks queries got and puts rows, one frame per step. */
export function syncPoke(
  pokeID: string,
  baseCookie: string | null,
  cookie: string,
  hashes: string[],
  rows: ReadonlyJSONValue[] = [],
): Step[] {
  return poke(pokeID, baseCookie, cookie, {
    gotQueriesPatch: hashes.map(gotPut),
    rowsPatch: rows,
  });
}

/** The error message zero-cache sends. */
export function errorMessage(body: JSONObject): Step {
  return {op: 'receive', message: ['error', body]};
}

export function mutate(
  mutation: string,
  mutator: string,
  args?: ReadonlyJSONValue,
): Step {
  return args === undefined
    ? {op: 'mutate', mutation, mutator}
    : {op: 'mutate', mutation, mutator, args};
}

export function advance(ms: number): Step {
  return {op: 'advance', ms};
}

/** A mutation result as the server reports it in a pushResponse. */
export function pushResponse(
  results: {id: number; result: JSONObject}[],
): Step {
  return {
    op: 'receive',
    message: [
      'pushResponse',
      {
        mutations: results.map(({id, result}) => ({
          id: {clientID: '$clientID', id},
          result,
        })),
      },
    ],
  };
}
