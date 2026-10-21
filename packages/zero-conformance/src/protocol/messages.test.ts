/**
 * Vectors for the zero-protocol wire messages
 * (`packages/zero-protocol/src/up.ts`, `down.ts`, `poke.ts`, `connect.ts`,
 * and `client-schema.ts`).
 *
 * The JS client parses downstream messages in valita's `passthrough` mode
 * (`#onMessage` in `packages/zero-client/src/client/zero.ts`). The Swift
 * client decodes them into types that drop unknown keys, as `strip` mode
 * does. So each case records whether `passthrough` accepts the message and,
 * if it does, the `strip` output, which the Swift types must encode back to.
 *
 * Mutation results are the exception. `mutationResultSchema`'s OK branch has
 * no discriminator and accepts every object, so `strip` turns every error
 * into `{}`. The client reads the `passthrough` output, and tells errors
 * apart with `'error' in result` (`mutation-tracker.ts`). That output is the
 * result as sent, unless it has `data`: then valita returns just `{data}`, as
 * its object matcher copies only known keys once a field's matcher returns a
 * value, which `jsonSchema` always does. So the expected output has each
 * result as the client reads it: `{error, message?, details?}` or `{data?}`.
 */
import {expect, test} from 'vitest';
import * as v from '../../../shared/src/valita.ts';
import {
  clientSchemaSchema,
  normalizeClientSchema,
} from '../../../zero-protocol/src/client-schema.ts';
import {
  decodeSecProtocols,
  encodeSecProtocols,
  initConnectionMessageSchema,
  type InitConnectionMessage,
} from '../../../zero-protocol/src/connect.ts';
import {downstreamSchema} from '../../../zero-protocol/src/down.ts';
import * as ErrorKind from '../../../zero-protocol/src/error-kind-enum.ts';
import {
  POKE_CHUNK_MESSAGE_TYPE,
  pokePartsSchema,
} from '../../../zero-protocol/src/poke.ts';
import {upstreamSchema} from '../../../zero-protocol/src/up.ts';
import {interestingStrings, Random, randomString} from '../random.ts';
import {emitGeneratedVectors, emitVectors} from '../vectors.ts';

type JSONValue = null | boolean | number | string | JSONValue[] | JSONObject;
type JSONObject = {[key: string]: JSONValue};
type Obj = Record<string, unknown>;

// Random values.

const numbers = [
  0,
  -0,
  1,
  -1,
  2,
  7,
  0.5,
  -2.25,
  2 ** 31,
  2 ** 53,
  -(2 ** 53),
  1e21,
  1e-7,
  5e-324,
  Number.MAX_VALUE,
];

function str(r: Random): string {
  return r.bool(0.3) ? r.pick(interestingStrings) : randomString(r);
}

function num(r: Random): number {
  return r.bool(0.7) ? r.pick(numbers) : r.int(100000);
}

function list<T>(r: Random, max: number, f: () => T): T[] {
  const n = r.int(max + 1);
  const out: T[] = [];
  for (let i = 0; i < n; i++) {
    out.push(f());
  }
  return out;
}

function record<T>(r: Random, max: number, f: () => T): {[key: string]: T} {
  const out: {[key: string]: T} = {};
  const n = r.int(max + 1);
  for (let i = 0; i < n; i++) {
    // NFC, so that no two keys are canonically equivalent: Swift's
    // `[String: JSONValue]` would take them for one key.
    out[str(r).normalize('NFC')] = f();
  }
  return out;
}

function json(r: Random, depth = 0): JSONValue {
  switch (r.int(depth >= 2 ? 4 : 6)) {
    case 0:
      return null;
    case 1:
      return r.bool();
    case 2:
      return num(r);
    case 3:
      return str(r);
    case 4:
      return list(r, 3, () => json(r, depth + 1));
    default:
      return record(r, 3, () => json(r, depth + 1));
  }
}

/** `{[key]: value()}` with probability `p`, to spread into an object. */
function opt(
  r: Random,
  key: string,
  value: () => JSONValue,
  p = 0.5,
): JSONObject {
  return r.bool(p) ? {[key]: value()} : {};
}

/** Keys no schema has, which `strip` drops. */
const extraKeys = ['zExtra', 'newField', '', '\u00e9', '\u{1f600}'];
let extraProbability = 0.15;

function extras(r: Random): JSONObject {
  return r.bool(extraProbability) ? {[r.pick(extraKeys)]: json(r, 1)} : {};
}

// Shared message parts.

const tables = ['issue', 'comment', 'user', '\u00e9t\u00e9'];
const columns = ['id', 'title', 'n', '\u{1f600}', ''];

function row(r: Random): JSONObject {
  const out: JSONObject = {};
  for (const c of columns) {
    if (r.bool()) {
      out[c] = json(r, 1);
    }
  }
  return out;
}

function primaryKeyValue(r: Random): JSONValue {
  return r.pick<JSONValue>(['a', '', 1, -0, 1.5, true, false, '\u{1f600}']);
}

function primaryKeyValues(r: Random): JSONObject {
  return {id: primaryKeyValue(r), ...opt(r, 'n', () => primaryKeyValue(r))};
}

function mutationID(r: Random): JSONObject {
  return {id: r.int(1000), clientID: str(r), ...extras(r)};
}

const conditions: JSONObject[] = [
  {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'id'},
    right: {type: 'literal', value: '1'},
  },
  {
    type: 'and',
    conditions: [
      {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          correlation: {parentField: ['ownerID'], childField: ['id']},
          subquery: {table: 'user'},
        },
      },
      {
        type: 'simple',
        op: 'IN',
        left: {type: 'column', name: 'n'},
        right: {type: 'literal', value: [1, 'a', true]},
      },
    ],
  },
  {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'ownerID'},
    right: {type: 'static', anchor: 'authData', field: ['user', 'sub']},
  },
];

const asts: JSONObject[] = [
  {table: 'issue'},
  {table: 'issue', where: conditions[0], orderBy: [['id', 'asc']], limit: 10},
  {
    table: 'issue',
    alias: 'i',
    schema: 'public',
    related: [
      {
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'comment',
          alias: 'comments',
          orderBy: [['id', 'desc']],
        },
        system: 'permissions',
        hidden: true,
      },
    ],
    start: {row: {id: 'a', n: null}, exclusive: true},
  },
  {table: 'issue', where: conditions[1]},
  {table: 'issue', where: conditions[2]},
];

function deleteClientsBody(r: Random): JSONObject {
  return {
    ...opt(r, 'clientIDs', () => list(r, 2, () => str(r))),
    ...opt(r, 'clientGroupIDs', () => list(r, 2, () => str(r))),
    ...extras(r),
  };
}

// Downstream messages.

function queriesPatchOp(r: Random): JSONObject {
  switch (r.int(4)) {
    case 0:
    case 1:
      return {
        op: 'put',
        hash: str(r),
        ...opt(r, 'ttl', () => num(r)),
        ...extras(r),
      };
    case 2:
      return {op: 'del', hash: str(r), ...extras(r)};
    default:
      return {op: 'clear', ...extras(r)};
  }
}

function rowPatchOp(r: Random): JSONObject {
  const tableName = r.pick(tables);
  switch (r.int(5)) {
    case 0:
    case 1:
      return {op: 'put', tableName, value: row(r), ...extras(r)};
    case 2:
      return {
        op: 'update',
        tableName,
        id: primaryKeyValues(r),
        // jsonObjectSchema checks `typeof v === 'object'`, so arrays pass.
        ...opt(r, 'merge', () =>
          r.bool(0.85) ? row(r) : list(r, 2, () => json(r, 1)),
        ),
        ...opt(r, 'constrain', () => list(r, 3, () => r.pick(columns))),
        ...extras(r),
      };
    case 3:
      return {op: 'del', tableName, id: primaryKeyValues(r), ...extras(r)};
    default:
      return {op: 'clear', ...extras(r)};
  }
}

function mutationResult(r: Random): JSONObject {
  switch (r.int(7)) {
    case 0:
      return {...extras(r)};
    case 1:
    case 2:
      return {data: json(r), ...extras(r)};
    case 3:
      return {
        error: 'app',
        ...opt(r, 'message', () => str(r)),
        ...opt(r, 'details', () => json(r)),
        ...extras(r),
      };
    case 4:
      return {
        error: r.pick(['oooMutation', 'alreadyProcessed']),
        ...opt(r, 'details', () => json(r)),
        ...extras(r),
      };
    default:
      // The OK branch accepts any object, so any `error` passes too.
      return {
        error: r.bool() ? json(r) : 'unknownError',
        ...opt(r, 'message', () => json(r)),
        ...opt(r, 'data', () => json(r)),
      };
  }
}

function mutationResponse(r: Random): JSONObject {
  return {id: mutationID(r), result: mutationResult(r), ...extras(r)};
}

function mutationPatchOp(r: Random): JSONObject {
  return r.bool(0.7)
    ? {op: 'put', mutation: mutationResponse(r), ...extras(r)}
    : {op: 'del', id: mutationID(r), ...extras(r)};
}

function pokePartBody(r: Random): JSONObject {
  return {
    pokeID: str(r),
    ...opt(r, 'lastMutationIDChanges', () => record(r, 3, () => r.int(1000))),
    ...opt(
      r,
      'desiredQueriesPatches',
      () => record(r, 2, () => list(r, 3, () => queriesPatchOp(r))),
      0.3,
    ),
    ...opt(
      r,
      'gotQueriesPatch',
      () => list(r, 3, () => queriesPatchOp(r)),
      0.3,
    ),
    ...opt(r, 'rowsPatch', () => list(r, 5, () => rowPatchOp(r)), 0.6),
    ...opt(
      r,
      'mutationsPatch',
      () => list(r, 3, () => mutationPatchOp(r)),
      0.4,
    ),
    ...extras(r),
  };
}

const backoffKinds: string[] = [
  ErrorKind.Rebalance,
  ErrorKind.Rehome,
  ErrorKind.ServerOverloaded,
];
const basicKinds: string[] = Object.values(ErrorKind).filter(
  k =>
    !backoffKinds.includes(k) &&
    k !== ErrorKind.PushFailed &&
    k !== ErrorKind.TransformFailed,
);
const pushServerReasons = [
  'database',
  'parse',
  'oooMutation',
  'unsupportedPushVersion',
  'internal',
];
const transformServerReasons = ['database', 'parse', 'internal'];
const zeroCacheReasons = ['timeout', 'parse', 'internal'];

function failureCause(r: Random, serverReasons: string[]): JSONObject {
  switch (r.int(3)) {
    case 0:
      return {origin: 'server', reason: r.pick(serverReasons)};
    case 1:
      return {
        origin: 'zeroCache',
        reason: 'http',
        status: r.pick([400, 401, 500, 503]),
        ...opt(r, 'bodyPreview', () => str(r)),
      };
    default:
      return {origin: 'zeroCache', reason: r.pick(zeroCacheReasons)};
  }
}

function errorBody(r: Random): JSONObject {
  const message = str(r);
  switch (r.int(4)) {
    case 0:
      return {
        kind: r.pick(basicKinds),
        message,
        ...opt(r, 'origin', () => r.pick(['server', 'zeroCache'])),
        ...extras(r),
      };
    case 1:
      return {
        kind: r.pick(backoffKinds),
        message,
        ...opt(r, 'minBackoffMs', () => num(r)),
        ...opt(r, 'maxBackoffMs', () => num(r)),
        ...opt(r, 'reconnectParams', () => record(r, 2, () => str(r))),
        ...opt(r, 'origin', () => 'zeroCache'),
        ...extras(r),
      };
    case 2:
      return {
        kind: ErrorKind.PushFailed,
        message,
        mutationIDs: list(r, 2, () => mutationID(r)),
        ...opt(r, 'details', () => json(r)),
        ...failureCause(r, pushServerReasons),
        ...extras(r),
      };
    default:
      return {
        kind: ErrorKind.TransformFailed,
        message,
        queryIDs: list(r, 2, () => str(r)),
        ...opt(r, 'details', () => json(r)),
        ...failureCause(r, transformServerReasons),
        ...extras(r),
      };
  }
}

function pushResponseBody(r: Random): JSONObject {
  const mutationIDs = () =>
    opt(r, 'mutationIDs', () => list(r, 2, () => mutationID(r)));
  switch (r.int(6)) {
    case 0:
    case 1:
      return {mutations: list(r, 3, () => mutationResponse(r)), ...extras(r)};
    case 2:
      return {error: 'unsupportedPushVersion', ...mutationIDs(), ...extras(r)};
    case 3:
      return {
        error: 'unsupportedSchemaVersion',
        ...mutationIDs(),
        ...extras(r),
      };
    case 4:
      return {
        error: 'http',
        status: r.pick([400, 500]),
        details: str(r),
        ...mutationIDs(),
        ...extras(r),
      };
    default:
      return {
        error: 'zeroPusher',
        details: str(r),
        ...mutationIDs(),
        ...extras(r),
      };
  }
}

function tdigest(r: Random): number[] {
  return [num(r), ...list(r, 3, () => num(r))];
}

function costEstimate(r: Random): JSONObject {
  return {
    startupCost: num(r),
    scanEst: num(r),
    cost: num(r),
    returnedRows: num(r),
    selectivity: num(r),
    ...opt(r, 'limit', () => num(r)),
    ...extras(r),
  };
}

function constraints(r: Random): JSONObject {
  return record(r, 2, () =>
    r.bool(0.2) ? null : record(r, 2, () => json(r, 1)),
  );
}

function planDebugEvent(r: Random): JSONObject {
  const attemptNumber = r.int(5);
  const joinStates = () =>
    list(r, 2, () => ({
      join: str(r),
      type: r.pick(['semi', 'flipped', 'unflippable', 'newType']),
    }));
  const branchPattern = () => list(r, 3, () => r.int(2));
  switch (r.int(9)) {
    case 0:
      return {type: 'attempt-start', attemptNumber, totalAttempts: r.int(5)};
    case 1:
      return {
        type: 'connection-costs',
        attemptNumber,
        costs: list(r, 2, () => ({
          connection: str(r),
          cost: num(r),
          costEstimate: costEstimate(r),
          pinned: r.bool(),
          constraints: constraints(r),
          constraintCosts: record(r, 2, () => costEstimate(r)),
        })),
      };
    case 2:
      return {
        type: 'connection-selected',
        attemptNumber,
        connection: str(r),
        cost: num(r),
        isRoot: r.bool(),
      };
    case 3:
      return {
        type: 'constraints-propagated',
        attemptNumber,
        connectionConstraints: list(r, 2, () => ({
          connection: str(r),
          constraints: constraints(r),
          constraintCosts: record(r, 1, () => costEstimate(r)),
        })),
      };
    case 4:
      return {
        type: 'plan-complete',
        attemptNumber,
        totalCost: num(r),
        flipPattern: r.int(8),
        joinStates: joinStates(),
      };
    case 5:
      return {type: 'plan-failed', attemptNumber, reason: str(r)};
    case 6:
      return {
        type: 'best-plan-selected',
        bestAttemptNumber: attemptNumber,
        totalCost: num(r),
        flipPattern: r.int(8),
        joinStates: joinStates(),
      };
    case 7:
      return {
        type: 'node-cost',
        ...opt(r, 'attemptNumber', () => attemptNumber),
        nodeType: r.pick([
          'connection',
          'join',
          'fan-in',
          'terminus',
          'newNode',
        ]),
        node: str(r),
        branchPattern: branchPattern(),
        downstreamChildSelectivity: num(r),
        costEstimate: costEstimate(r),
        ...opt(r, 'filters', () => r.pick(conditions)),
        ...opt(r, 'ordering', () => [['id', r.pick(['asc', 'desc'])]]),
        ...opt(r, 'joinType', () => r.pick(['semi', 'flipped'])),
        ...extras(r),
      };
    default:
      return {
        type: 'node-constraint',
        ...opt(r, 'attemptNumber', () => attemptNumber),
        nodeType: 'join',
        node: str(r),
        branchPattern: branchPattern(),
        ...opt(r, 'constraint', () =>
          r.bool(0.3) ? null : record(r, 2, () => json(r, 1)),
        ),
        from: str(r),
      };
  }
}

function analyzeQueryResult(r: Random): JSONObject {
  const rowsBySource = () =>
    record(r, 2, () => record(r, 2, () => list(r, 2, () => row(r))));
  const countsBySource = () =>
    record(r, 2, () => record(r, 2, () => r.int(100)));
  return {
    warnings: list(r, 2, () => str(r)),
    ...opt(
      r,
      'syncedRows',
      () => record(r, 2, () => list(r, 2, () => row(r))),
      0.3,
    ),
    syncedRowCount: r.int(100),
    start: num(r),
    end: num(r),
    ...opt(r, 'elapsed', () => num(r), 0.3),
    ...opt(r, 'afterPermissions', () => str(r), 0.3),
    ...opt(r, 'vendedRowCounts', countsBySource, 0.3),
    ...opt(r, 'vendedRows', rowsBySource, 0.2),
    ...opt(
      r,
      'sqlitePlans',
      () => record(r, 2, () => list(r, 2, () => str(r))),
      0.3,
    ),
    ...opt(r, 'readRows', rowsBySource, 0.2),
    ...opt(r, 'readRowCountsByQuery', countsBySource, 0.2),
    ...opt(r, 'readRowCount', () => r.int(100), 0.3),
    ...opt(r, 'dbScansByQuery', countsBySource, 0.2),
    ...opt(r, 'joinPlans', () => list(r, 3, () => planDebugEvent(r)), 0.4),
    ...extras(r),
  };
}

function inspectQueryRow(r: Random): JSONObject {
  return {
    clientID: str(r),
    queryID: str(r),
    ast: r.bool() ? r.pick(asts) : null,
    name: r.bool() ? str(r) : null,
    args: r.bool() ? list(r, 2, () => json(r, 1)) : null,
    got: r.bool(),
    deleted: r.bool(),
    ttl: num(r),
    inactivatedAt: r.bool() ? num(r) : null,
    rowCount: r.int(100),
    ...opt(r, 'metrics', () =>
      r.bool(0.3)
        ? null
        : {
            ...opt(r, 'query-hydration-server-ms', () => num(r)),
            'query-update-server': tdigest(r),
          },
    ),
    ...extras(r),
  };
}

function inspectDownBody(r: Random): JSONObject {
  const id = str(r);
  switch (r.int(6)) {
    case 0:
      return {
        id,
        op: 'queries',
        value: list(r, 2, () => inspectQueryRow(r)),
        ...extras(r),
      };
    case 1:
      return {
        id,
        op: 'metrics',
        value: {
          'query-materialization-server': tdigest(r),
          'query-update-server': tdigest(r),
        },
      };
    case 2:
      return {id, op: 'version', value: str(r)};
    case 3:
      return {id, op: 'authenticated', value: r.bool()};
    case 4:
      return {
        id,
        op: 'analyze-query',
        value: analyzeQueryResult(r),
        ...extras(r),
      };
    default:
      return {id, op: 'error', value: str(r)};
  }
}

function erroredQuery(r: Random): JSONObject {
  return r.bool()
    ? {
        error: 'app',
        id: str(r),
        name: str(r),
        ...opt(r, 'message', () => str(r)),
        ...opt(r, 'details', () => json(r)),
        ...extras(r),
      }
    : {
        error: 'parse',
        id: str(r),
        name: str(r),
        message: str(r),
        ...opt(r, 'details', () => json(r)),
        ...extras(r),
      };
}

function downstreamMessage(r: Random): JSONValue {
  switch (r.int(14)) {
    case 0:
      return [
        'connected',
        {wsid: str(r), ...opt(r, 'timestamp', () => num(r)), ...extras(r)},
      ];
    case 1:
      return ['error', errorBody(r)];
    case 2:
      return ['pong', {...extras(r)}];
    case 3:
      return [
        'pokeStart',
        {
          pokeID: str(r),
          baseCookie: r.bool(0.2) ? null : str(r),
          ...opt(r, 'schemaVersions', () => ({
            minSupportedVersion: r.int(3),
            maxSupportedVersion: r.int(9),
            ...extras(r),
          })),
          ...opt(r, 'timestamp', () => num(r)),
          ...extras(r),
        },
      ];
    case 4:
    case 5:
    case 6:
      return ['pokePart', pokePartBody(r)];
    case 7:
      return [
        'pokeEnd',
        {
          pokeID: str(r),
          cookie: str(r),
          ...opt(r, 'cancel', () => r.bool()),
          ...extras(r),
        },
      ];
    case 8:
      return [
        'pull',
        {
          cookie: str(r),
          requestID: str(r),
          lastMutationIDChanges: record(r, 3, () => r.int(1000)),
          ...extras(r),
        },
      ];
    case 9:
      return ['deleteClients', deleteClientsBody(r)];
    case 10:
    case 11:
      return ['pushResponse', pushResponseBody(r)];
    case 12:
      return ['inspect', inspectDownBody(r)];
    default:
      return ['transformError', list(r, 3, () => erroredQuery(r))];
  }
}

// Upstream messages.

function upQueriesPatchOp(r: Random): JSONObject {
  switch (r.int(5)) {
    case 0:
    case 1:
    case 2:
      return {
        op: 'put',
        hash: str(r),
        ...opt(r, 'ttl', () => num(r)),
        ...opt(r, 'ast', () => r.pick(asts), 0.6),
        ...opt(r, 'name', () => str(r), 0.3),
        ...opt(r, 'args', () => list(r, 2, () => json(r, 1)), 0.3),
        ...extras(r),
      };
    case 3:
      return {op: 'del', hash: str(r), ...extras(r)};
    default:
      return {op: 'clear', ...extras(r)};
  }
}

const valueTypes = ['string', 'number', 'boolean', 'null', 'json'];
// Names whose UTF-16 order differs from their code point order. None is an
// array index, which JS objects order first, and no two are canonically
// equivalent, which Swift dictionaries would take for one key.
const schemaNames = [
  'issue',
  'Issue',
  'comment',
  'a',
  'ab',
  'b',
  'e',
  '_',
  '',
  '\u00e9',
  '\ue000',
  '\u{1f600}',
  '\u{1d49c}',
];

function pickSome(r: Random, names: readonly string[], max: number): string[] {
  const out: string[] = [];
  for (let i = names.length === 0 ? 0 : r.int(max + 1); i > 0; i--) {
    const name = r.pick(names);
    if (!out.includes(name)) {
      out.push(name);
    }
  }
  return out;
}

function clientSchema(r: Random): JSONObject {
  const schemaTables: JSONObject = {};
  for (const name of pickSome(r, schemaNames, 4)) {
    const columnNames = pickSome(r, schemaNames, 4);
    const schemaColumns: JSONObject = {};
    for (const c of columnNames) {
      schemaColumns[c] = {type: r.pick(valueTypes), ...extras(r)};
    }
    schemaTables[name] = {
      columns: schemaColumns,
      primaryKey: pickSome(r, columnNames, 3),
      ...extras(r),
    };
  }
  return {tables: schemaTables, ...extras(r)};
}

function initConnectionBody(r: Random): JSONObject {
  return {
    desiredQueriesPatch: list(r, 3, () => upQueriesPatchOp(r)),
    ...opt(r, 'clientSchema', () => clientSchema(r), 0.3),
    ...opt(r, 'deleted', () => deleteClientsBody(r), 0.2),
    ...opt(r, 'userPushURL', () => str(r), 0.3),
    ...opt(r, 'userPushHeaders', () => record(r, 2, () => str(r)), 0.3),
    ...opt(r, 'userQueryURL', () => str(r), 0.3),
    ...opt(r, 'userQueryHeaders', () => record(r, 2, () => str(r)), 0.3),
    ...opt(r, 'activeClients', () => list(r, 3, () => str(r)), 0.3),
    ...opt(r, 'traceparent', () => str(r), 0.2),
    ...extras(r),
  };
}

function crudOp(r: Random): JSONObject {
  const op = r.pick(['insert', 'upsert', 'update', 'delete']);
  return {
    op,
    tableName: r.pick(tables),
    primaryKey: r.bool(0.8) ? ['id'] : ['id', 'n'],
    value: op === 'delete' ? primaryKeyValues(r) : row(r),
    ...extras(r),
  };
}

function mutation(r: Random): JSONObject {
  const base = {id: r.int(1000), clientID: str(r), timestamp: num(r)};
  return r.bool(0.3)
    ? {
        type: 'crud',
        ...base,
        name: '_zero_crud',
        args: [{ops: list(r, 3, () => crudOp(r)), ...extras(r)}],
        ...extras(r),
      }
    : {
        type: 'custom',
        ...base,
        name: str(r),
        args: list(r, 3, () => json(r, 1)),
        ...extras(r),
      };
}

function inspectUpBody(r: Random): JSONObject {
  const id = str(r);
  switch (r.int(5)) {
    case 0:
      return {
        id,
        op: 'queries',
        ...opt(r, 'clientID', () => str(r)),
        ...extras(r),
      };
    case 1:
      return {id, op: 'metrics', ...extras(r)};
    case 2:
      return {id, op: 'version', ...extras(r)};
    case 3:
      return {id, op: 'authenticate', value: str(r), ...extras(r)};
    default:
      return {
        id,
        op: 'analyze-query',
        ...opt(r, 'value', () => r.pick(asts), 0.3),
        ...opt(r, 'options', () => ({
          ...opt(r, 'vendedRows', () => r.bool()),
          ...opt(r, 'syncedRows', () => r.bool()),
          ...opt(r, 'joinPlans', () => r.bool()),
          ...extras(r),
        })),
        ...opt(r, 'ast', () => r.pick(asts), 0.5),
        ...opt(r, 'name', () => str(r), 0.3),
        ...opt(r, 'args', () => list(r, 2, () => json(r, 1)), 0.3),
        ...extras(r),
      };
  }
}

function upstreamMessage(r: Random): JSONValue {
  switch (r.int(13)) {
    case 0:
    case 1:
      return ['initConnection', initConnectionBody(r)];
    case 2:
      return ['ping', {...extras(r)}];
    case 3:
      return ['deleteClients', deleteClientsBody(r)];
    case 4:
      return [
        'changeDesiredQueries',
        {
          desiredQueriesPatch: list(r, 3, () => upQueriesPatchOp(r)),
          ...opt(r, 'traceparent', () => str(r)),
          ...extras(r),
        },
      ];
    case 5:
      return [
        'pull',
        {
          clientGroupID: str(r),
          cookie: r.bool(0.3) ? null : str(r),
          requestID: str(r),
          ...extras(r),
        },
      ];
    case 6:
      return ['updateAuth', {auth: str(r), ...extras(r)}];
    case 7:
    case 8:
      return [
        'push',
        {
          clientGroupID: str(r),
          mutations: list(r, 3, () => mutation(r)),
          pushVersion: 1,
          ...opt(r, 'schemaVersion', () => r.int(5)),
          timestamp: num(r),
          requestID: str(r),
          ...opt(r, 'auth', () => str(r), 0.2),
          ...opt(r, 'traceparent', () => str(r), 0.2),
          ...extras(r),
        },
      ];
    case 9:
      return ['closeConnection', list(r, 2, () => json(r, 1))];
    case 10:
      return ['inspect', inspectUpBody(r)];
    case 11:
      return ['ackMutationResponses', mutationID(r)];
    default:
      // The union is open: a tag this build doesn't know parses as is.
      return [
        r.pick(['newMessage', '', 'Ping']),
        ...list(r, 2, () => json(r, 1)),
      ];
  }
}

// Invalid messages: random edits of valid ones. valita decides which still
// parse.

type Path = (string | number)[];

function paths(value: JSONValue, path: Path, out: Path[]): Path[] {
  out.push(path);
  if (Array.isArray(value)) {
    value.forEach((e, i) => paths(e, [...path, i], out));
  } else if (value !== null && typeof value === 'object') {
    for (const [k, e] of Object.entries(value)) {
      paths(e, [...path, k], out);
    }
  }
  return out;
}

const replacements: JSONValue[] = [
  null,
  true,
  0,
  -1.5,
  '',
  'bogus',
  [],
  [1],
  {},
  {zExtra: 1},
];
const literals = [
  'put',
  'del',
  'clear',
  'update',
  'insert',
  'delete',
  'app',
  'parse',
  'http',
  'timeout',
  'server',
  'zeroCache',
  'client',
  'PushFailed',
  'TransformFailed',
  'Rebalance',
  'Internal',
  'crud',
  'custom',
  '_zero_crud',
  'pokePart',
  'pull',
  'queries',
  'error',
  'simple',
  'and',
  'column',
  'asc',
];
const editKeys = [
  'error',
  'op',
  'type',
  'kind',
  'origin',
  'reason',
  'data',
  'message',
  'mutations',
  'status',
  'zExtra',
];

function replacement(r: Random): JSONValue {
  return structuredClone(r.pick(replacements));
}

function mutate(r: Random, message: JSONValue): JSONValue {
  // Boxed, so the root can be replaced too.
  const box: JSONValue[] = [structuredClone(message)];
  const path = r.pick(paths(box[0], [0], []));
  let parent = box as JSONValue;
  for (const k of path.slice(0, -1)) {
    parent = (parent as Record<string | number, JSONValue>)[k];
  }
  const container = parent as Record<string | number, JSONValue>;
  // Every path starts with the box index.
  const key = path.at(-1) as string | number;
  const node = container[key];
  switch (r.int(4)) {
    case 0:
      container[key] = replacement(r);
      break;
    case 1:
      if (path.length === 1) {
        container[key] = replacement(r);
      } else if (Array.isArray(container)) {
        container.splice(key as number, 1);
      } else {
        delete container[key];
      }
      break;
    case 2:
      if (Array.isArray(node)) {
        node.push(
          node.length > 0 && r.bool()
            ? structuredClone(node[0])
            : replacement(r),
        );
      } else if (node !== null && typeof node === 'object') {
        node[r.pick(editKeys)] = r.bool() ? r.pick(literals) : replacement(r);
      } else {
        container[key] = replacement(r);
      }
      break;
    default:
      container[key] =
        typeof node === 'number' ? String(node) : r.pick(literals);
      break;
  }
  return box[0];
}

// Expected results.

/** A result as the JS client reads it: `'error' in result`. */
function readResult(result: Obj): JSONObject {
  const keys = 'error' in result ? ['error', 'message', 'details'] : ['data'];
  const out: JSONObject = {};
  for (const k of keys) {
    if (k in result) {
      out[k] = result[k] as JSONValue;
    }
  }
  return out;
}

function readPokePart(strip: Obj, passthrough: Obj): Obj {
  const patch = strip['mutationsPatch'] as Obj[] | undefined;
  if (patch === undefined) {
    return strip;
  }
  const raw = passthrough['mutationsPatch'] as Obj[];
  return {
    ...strip,
    mutationsPatch: patch.map((op, i) =>
      op['op'] === 'put'
        ? {
            ...op,
            mutation: {
              ...(op['mutation'] as Obj),
              result: readResult((raw[i]['mutation'] as Obj)['result'] as Obj),
            },
          }
        : op,
    ),
  };
}

function readDownstream(strip: unknown, passthrough: unknown): JSONValue {
  const [tag, body] = strip as [string, Obj];
  const [, raw] = passthrough as [string, Obj];
  if (tag === 'pokePart') {
    return [tag, readPokePart(body, raw)] as JSONValue;
  }
  if (tag === 'pushResponse' && 'mutations' in body) {
    const rawMutations = raw['mutations'] as Obj[];
    return [
      tag,
      {
        mutations: (body['mutations'] as Obj[]).map((m, i) => ({
          ...m,
          result: readResult(rawMutations[i]['result'] as Obj),
        })),
      },
    ] as JSONValue;
  }
  return strip as JSONValue;
}

type MessageCase = {message: JSONValue; ok: boolean; output?: JSONValue};

function messageCase(
  message: JSONValue,
  schema: v.Type<unknown>,
  read: (strip: unknown, passthrough: unknown) => JSONValue,
): MessageCase {
  const passthrough = v.test(message, schema, 'passthrough');
  const strip = v.test(message, schema, 'strip');
  // The modes differ only in what they return.
  expect(strip.ok).toBe(passthrough.ok);
  if (!passthrough.ok || !strip.ok) {
    return {message, ok: false};
  }
  return {message, ok: true, output: read(strip.value, passthrough.value)};
}

function downstreamCase(message: JSONValue): MessageCase {
  return messageCase(message, downstreamSchema, readDownstream);
}

function upstreamCase(message: JSONValue): MessageCase {
  return messageCase(message, upstreamSchema, strip => strip as JSONValue);
}

function corpus(
  r: Random,
  hand: JSONValue[],
  random: (r: Random) => JSONValue,
  toCase: (message: JSONValue) => MessageCase,
  validCount: number,
  mutatedCount: number,
): MessageCase[] {
  const cases = hand.map(toCase);
  const valid: JSONValue[] = [];
  for (let i = 0; i < validCount; i++) {
    const message = random(r);
    const c = toCase(message);
    expect(c.ok, JSON.stringify(message)).toBe(true);
    cases.push(c);
    valid.push(message);
  }
  for (let i = 0; i < mutatedCount; i++) {
    let message = mutate(r, r.pick(valid));
    if (r.bool(0.3)) {
      message = mutate(r, message);
    }
    cases.push(toCase(message));
  }
  return cases;
}

// Hand-written downstream messages, for each variant and its edges.

const mid = (id: number): JSONObject => ({id, clientID: 'c'});

const handDownstream: JSONValue[] = [
  ['connected', {wsid: 'ws1'}],
  ['connected', {wsid: 'ws1', timestamp: 1700000000000}],
  // An optional field may be absent, but not null.
  ['connected', {wsid: 'ws1', timestamp: null}],
  ['connected', {wsid: 1}],
  ['connected', {}],
  ['connected', {wsid: 'ws1'}, 'extra'],
  ['connected'],
  [],
  'connected',
  null,
  {},
  5,
  ['unknownMessage', {}],
  [1, {}],
  ['pong', {}],
  ['pong', {zExtra: 1}],
  ['pong', []],
  ['pong', null],
  ...basicKinds.map(kind => ['error', {kind, message: 'm'}]),
  ['error', {kind: 'InvalidMessage', message: 'm', origin: 'server'}],
  ['error', {kind: 'InvalidMessage', message: 'm', origin: 'zeroCache'}],
  ['error', {kind: 'InvalidMessage', message: 'm', origin: 'client'}],
  ['error', {kind: 'InvalidMessage'}],
  ['error', {message: 'm'}],
  ['error', {kind: 'NoSuchKind', message: 'm'}],
  ...backoffKinds.map(kind => [
    'error',
    {
      kind,
      message: 'm',
      minBackoffMs: 100,
      maxBackoffMs: 1000,
      reconnectParams: {'a': 'b', '': ''},
      origin: 'zeroCache',
    },
  ]),
  ['error', {kind: 'Rebalance', message: 'm', origin: 'server'}],
  ['error', {kind: 'Rehome', message: 'm', reconnectParams: {a: 1}}],
  ...[...pushServerReasons, 'http', 'timeout'].map(reason => [
    'error',
    {
      kind: 'PushFailed',
      message: 'm',
      mutationIDs: [mid(1)],
      origin: 'server',
      reason,
    },
  ]),
  ...[...zeroCacheReasons, 'database', 'oooMutation'].map(reason => [
    'error',
    {
      kind: 'PushFailed',
      message: 'm',
      mutationIDs: [],
      origin: 'zeroCache',
      reason,
    },
  ]),
  [
    'error',
    {
      kind: 'PushFailed',
      message: 'm',
      mutationIDs: [mid(1), mid(2)],
      origin: 'zeroCache',
      reason: 'http',
      status: 500,
      bodyPreview: '<html>',
      details: {a: [1, null]},
    },
  ],
  [
    'error',
    {
      kind: 'PushFailed',
      message: 'm',
      mutationIDs: [],
      origin: 'zeroCache',
      reason: 'http',
    },
  ],
  [
    'error',
    {kind: 'PushFailed', message: 'm', origin: 'server', reason: 'parse'},
  ],
  [
    'error',
    {
      kind: 'PushFailed',
      message: 'm',
      mutationIDs: [],
      origin: 'client',
      reason: 'parse',
    },
  ],
  ...[...transformServerReasons, 'oooMutation', 'http'].map(reason => [
    'error',
    {
      kind: 'TransformFailed',
      message: 'm',
      queryIDs: ['q'],
      origin: 'server',
      reason,
    },
  ]),
  ...[...zeroCacheReasons, 'database'].map(reason => [
    'error',
    {
      kind: 'TransformFailed',
      message: 'm',
      queryIDs: [],
      origin: 'zeroCache',
      reason,
    },
  ]),
  [
    'error',
    {
      kind: 'TransformFailed',
      message: 'm',
      queryIDs: ['q1', 'q2'],
      origin: 'zeroCache',
      reason: 'http',
      status: 404,
      details: 'd',
    },
  ],
  [
    'error',
    {
      kind: 'TransformFailed',
      message: 'm',
      mutationIDs: [],
      origin: 'server',
      reason: 'parse',
    },
  ],
  ['pokeStart', {pokeID: 'p', baseCookie: null}],
  [
    'pokeStart',
    {
      pokeID: 'p',
      baseCookie: '1',
      schemaVersions: {minSupportedVersion: 1, maxSupportedVersion: 2},
      timestamp: 5,
    },
  ],
  // `baseCookie` is nullable, not optional.
  ['pokeStart', {pokeID: 'p'}],
  ['pokeStart', {pokeID: 'p', baseCookie: 1}],
  [
    'pokeStart',
    {pokeID: 'p', baseCookie: '1', schemaVersions: {minSupportedVersion: 1}},
  ],
  ['pokePart', {pokeID: 'p'}],
  ['pokePart', {}],
  [
    'pokePart',
    {
      pokeID: 'p',
      rowsPatch: [
        {op: 'put', tableName: 't', value: {id: 1, x: null, j: {a: [1]}}},
        {
          op: 'update',
          tableName: 't',
          id: {id: 1},
          merge: {x: 2},
          constrain: ['id', 'x'],
        },
        {op: 'update', tableName: 't', id: {id: 'a', b: true}, merge: [1, 2]},
        {op: 'update', tableName: 't', id: {id: 'a'}},
        {op: 'del', tableName: 't', id: {id: 1.5}},
        {op: 'del', tableName: 't', id: {}},
        {op: 'clear'},
      ],
    },
  ],
  [
    'pokePart',
    {
      pokeID: 'p',
      rowsPatch: [{op: 'update', tableName: 't', id: {id: 1}, merge: 'x'}],
    },
  ],
  [
    'pokePart',
    {
      pokeID: 'p',
      rowsPatch: [{op: 'update', tableName: 't', id: {id: 1}, merge: null}],
    },
  ],
  [
    'pokePart',
    {pokeID: 'p', rowsPatch: [{op: 'del', tableName: 't', id: {id: null}}]},
  ],
  ['pokePart', {pokeID: 'p', rowsPatch: [{op: 'del', tableName: 't', id: []}]}],
  [
    'pokePart',
    {pokeID: 'p', rowsPatch: [{op: 'put', tableName: 't', value: []}]},
  ],
  ['pokePart', {pokeID: 'p', rowsPatch: [{op: 'put', tableName: 't'}]}],
  [
    'pokePart',
    {pokeID: 'p', rowsPatch: [{op: 'upsert', tableName: 't', value: {}}]},
  ],
  ['pokePart', {pokeID: 'p', rowsPatch: {}}],
  [
    'pokePart',
    {
      pokeID: 'p',
      lastMutationIDChanges: {c1: 5, c2: 0},
      desiredQueriesPatches: {
        c1: [
          {op: 'put', hash: 'h', ttl: 1000},
          {op: 'del', hash: 'h2'},
          {op: 'clear'},
        ],
        c2: [],
      },
      gotQueriesPatch: [{op: 'put', hash: 'h'}],
    },
  ],
  ['pokePart', {pokeID: 'p', lastMutationIDChanges: {c1: '5'}}],
  [
    'pokePart',
    {pokeID: 'p', gotQueriesPatch: [{op: 'put', hash: 'h', ttl: null}]},
  ],
  ['pokePart', {pokeID: 'p', gotQueriesPatch: [{op: 'add', hash: 'h'}]}],
  [
    'pokePart',
    {
      pokeID: 'p',
      mutationsPatch: [
        {op: 'put', mutation: {id: mid(1), result: {}}},
        {op: 'put', mutation: {id: mid(2), result: {data: {a: 1}}}},
        {op: 'put', mutation: {id: mid(3), result: {data: null}}},
        {
          op: 'put',
          mutation: {
            id: mid(4),
            result: {error: 'app', message: 'boom', details: [1]},
          },
        },
        {op: 'put', mutation: {id: mid(5), result: {error: 'oooMutation'}}},
        {
          op: 'put',
          mutation: {
            id: mid(6),
            result: {error: 'alreadyProcessed', details: 'x'},
          },
        },
        // Any object is a result, so any `error` is an error.
        {op: 'put', mutation: {id: mid(7), result: {error: 5, message: 6}}},
        {op: 'put', mutation: {id: mid(8), result: {error: null}}},
        // With `data`, valita's passthrough output is just `{data}`: OK.
        {
          op: 'put',
          mutation: {id: mid(9), result: {data: 1, error: 'app', zExtra: true}},
        },
        {op: 'put', mutation: {id: mid(10), result: {zExtra: true}}},
        {op: 'del', id: mid(11)},
      ],
    },
  ],
  [
    'pokePart',
    {
      pokeID: 'p',
      mutationsPatch: [{op: 'put', mutation: {id: mid(1), result: null}}],
    },
  ],
  [
    'pokePart',
    {
      pokeID: 'p',
      mutationsPatch: [{op: 'put', mutation: {id: mid(1), result: []}}],
    },
  ],
  [
    'pokePart',
    {pokeID: 'p', mutationsPatch: [{op: 'put', mutation: {id: mid(1)}}]},
  ],
  ['pokePart', {pokeID: 'p', mutationsPatch: [{op: 'del', id: {id: 1}}]}],
  ['pokeEnd', {pokeID: 'p', cookie: '2'}],
  ['pokeEnd', {pokeID: 'p', cookie: '', cancel: true}],
  ['pokeEnd', {pokeID: 'p', cookie: null}],
  ['pokeEnd', {pokeID: 'p', cookie: '2', cancel: 'yes'}],
  ['pull', {cookie: '1', requestID: 'r', lastMutationIDChanges: {}}],
  ['pull', {cookie: '1', requestID: 'r', lastMutationIDChanges: {c: 3}}],
  ['pull', {cookie: null, requestID: 'r', lastMutationIDChanges: {}}],
  ['pull', {cookie: '1', requestID: 'r'}],
  ['deleteClients', {}],
  ['deleteClients', {clientIDs: ['a'], clientGroupIDs: ['b', '']}],
  ['deleteClients', {clientIDs: null}],
  ['deleteClients', {clientIDs: [1]}],
  ['pushResponse', {mutations: []}],
  [
    'pushResponse',
    {
      mutations: [
        {id: mid(1), result: {error: 'app'}},
        {id: mid(2), result: {data: 'x', zExtra: 1}},
      ],
    },
  ],
  ['pushResponse', {error: 'unsupportedPushVersion'}],
  ['pushResponse', {error: 'unsupportedSchemaVersion', mutationIDs: [mid(1)]}],
  ['pushResponse', {error: 'http', status: 500, details: 'x'}],
  ['pushResponse', {error: 'http', status: 500}],
  ['pushResponse', {error: 'zeroPusher', details: 'x', mutationIDs: []}],
  ['pushResponse', {error: 'zeroPusher', details: 'x', mutationIDs: null}],
  ['pushResponse', {error: 'other'}],
  // Not OK, so it must be an error.
  ['pushResponse', {mutations: 'x', error: 'zeroPusher', details: 'd'}],
  ['pushResponse', {mutations: [{id: mid(1)}]}],
  ['pushResponse', {}],
  ['inspect', {id: '1', op: 'version', value: '1.0'}],
  ['inspect', {id: '1', op: 'authenticated', value: true}],
  ['inspect', {id: '1', op: 'error', value: 'nope', zExtra: 1}],
  [
    'inspect',
    {
      id: '1',
      op: 'metrics',
      value: {
        'query-materialization-server': [1],
        'query-update-server': [1, 2, 3],
      },
    },
  ],
  [
    'inspect',
    {
      id: '1',
      op: 'metrics',
      value: {'query-materialization-server': [], 'query-update-server': [1]},
    },
  ],
  [
    'inspect',
    {
      id: '1',
      op: 'queries',
      value: [
        {
          clientID: 'c',
          queryID: 'q',
          ast: asts[1],
          name: null,
          args: null,
          got: true,
          deleted: false,
          ttl: 1000,
          inactivatedAt: null,
          rowCount: 3,
          metrics: null,
        },
        {
          clientID: 'c',
          queryID: 'q2',
          ast: null,
          name: 'issues',
          args: [1, {a: 'b'}],
          got: false,
          deleted: true,
          ttl: 0,
          inactivatedAt: 5,
          rowCount: 0,
          metrics: {'query-update-server': [1.5]},
          zExtra: 1,
        },
      ],
    },
  ],
  [
    'inspect',
    {
      id: '1',
      op: 'queries',
      value: [
        {
          clientID: 'c',
          queryID: 'q',
          ast: {table: 1},
          name: null,
          args: null,
          got: true,
          deleted: false,
          ttl: 1000,
          inactivatedAt: null,
          rowCount: 3,
        },
      ],
    },
  ],
  [
    'inspect',
    {
      id: '1',
      op: 'analyze-query',
      value: {warnings: [], syncedRowCount: 0, start: 0, end: 1},
    },
  ],
  [
    'inspect',
    {
      id: '1',
      op: 'analyze-query',
      value: {
        warnings: ['w'],
        syncedRowCount: 0,
        start: 0,
        end: 1,
        joinPlans: [{type: 'plan-failed', attemptNumber: 1}],
      },
    },
  ],
  ['inspect', {id: '1', op: 'unknown', value: 1}],
  ['inspect', {op: 'version', value: '1'}],
  ['transformError', []],
  [
    'transformError',
    [
      {error: 'app', id: 'q', name: 'n'},
      {error: 'parse', id: 'q', name: 'n', message: 'm', details: {}},
    ],
  ],
  ['transformError', [{error: 'parse', id: 'q', name: 'n'}]],
  ['transformError', [{error: 'other', id: 'q', name: 'n', message: 'm'}]],
  ['transformError', {}],
];

// Hand-written upstream messages.

/** An AST whose where clause nests `n` conjunctions. */
function nestedConditions(n: number): JSONObject {
  let where: JSONObject = conditions[0];
  for (let i = 0; i < n; i++) {
    where = {type: 'and', conditions: [where]};
  }
  return {table: 'issue', where};
}

/** An AST with `n` levels of related subqueries. */
function nestedRelated(n: number): JSONObject {
  let ast: JSONObject = {table: 'issue'};
  for (let i = 0; i < n; i++) {
    ast = {
      table: 'issue',
      related: [
        {correlation: {parentField: ['id'], childField: ['id']}, subquery: ast},
      ],
    };
  }
  return ast;
}

const put = (ast: JSONValue): JSONValue => [
  'changeDesiredQueries',
  {desiredQueriesPatch: [{op: 'put', hash: 'h', ast}]},
];

const crud = (fields: JSONObject): JSONValue => [
  'push',
  {
    clientGroupID: 'g',
    mutations: [
      {
        type: 'crud',
        id: 1,
        clientID: 'c',
        name: '_zero_crud',
        args: [
          {
            ops: [
              {
                op: 'insert',
                tableName: 'issue',
                primaryKey: ['id'],
                value: {id: 'a'},
              },
            ],
          },
        ],
        timestamp: 1,
        ...fields,
      },
    ],
    pushVersion: 1,
    timestamp: 1,
    requestID: 'r',
  },
];

// At most MAX_AST_DEPTH (50) levels. Emitted as text: pretty-printed, the
// indentation alone would take megabytes.
const deepUpstream: JSONValue[] = [
  put(nestedConditions(49)),
  put(nestedConditions(50)),
  put(nestedConditions(51)),
  put(nestedRelated(50)),
  put(nestedRelated(51)),
];

type TextCase = {messageText: string; ok: boolean; outputText?: string};

function asText(c: MessageCase): TextCase {
  return {
    messageText: JSON.stringify(c.message),
    ok: c.ok,
    ...(c.output === undefined ? {} : {outputText: JSON.stringify(c.output)}),
  };
}

const handUpstream: JSONValue[] = [
  ['ping', {}],
  ['ping', {zExtra: 1}],
  ['ping'],
  ['ping', {}, 1],
  ['ping', []],
  // A tag this build doesn't know, of any length.
  ['newMessage'],
  ['newMessage', 1, 2, 3],
  [''],
  // The tag must be a string.
  [1, {}],
  [null],
  [true, {}],
  [['ping'], {}],
  [],
  'ping',
  {},
  ['initConnection', {desiredQueriesPatch: []}],
  [
    'initConnection',
    {
      desiredQueriesPatch: [
        {op: 'put', hash: 'h1', ttl: 300000, ast: asts[2]},
        {op: 'put', hash: 'h2', name: 'issues', args: [1, 'a', {b: null}]},
        {op: 'del', hash: 'h3'},
        {op: 'clear'},
      ],
      clientSchema: {
        tables: {
          issue: {
            columns: {id: {type: 'string'}, n: {type: 'number'}},
            primaryKey: ['id'],
          },
        },
      },
      deleted: {clientIDs: ['a'], clientGroupIDs: []},
      userPushURL: 'https://example.com/push',
      userPushHeaders: {authorization: 'Bearer x'},
      userQueryURL: 'https://example.com/query',
      userQueryHeaders: {},
      activeClients: ['c1', 'c2'],
      traceparent: '00-abc-def-01',
    },
  ],
  [
    'initConnection',
    {
      desiredQueriesPatch: [],
      clientSchema: {
        tables: {issue: {columns: {id: {type: 'date'}}, primaryKey: ['id']}},
      },
    },
  ],
  [
    'initConnection',
    {desiredQueriesPatch: [], clientSchema: {tables: {issue: {columns: {}}}}},
  ],
  ['initConnection', {desiredQueriesPatch: [], userPushHeaders: {a: 1}}],
  ['initConnection', {desiredQueriesPatch: [], activeClients: 'c1'}],
  ['initConnection', {}],
  // ASTs from clients are parsed strictly, even inside a lenient parse.
  put({table: 'issue', zExtra: 1}),
  put({
    table: 'issue',
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'column', name: 'a', zExtra: 1},
      right: {type: 'literal', value: 1},
    },
  }),
  put({table: 'issue', where: {type: 'and', conditions: [], zExtra: 1}}),
  put({
    table: 'issue',
    related: [
      {
        correlation: {parentField: ['id'], childField: ['id'], zExtra: 1},
        subquery: {table: 'comment'},
      },
    ],
  }),
  put({
    table: 'issue',
    related: [
      {
        correlation: {parentField: ['id'], childField: ['id']},
        subquery: {table: 'comment', zExtra: 1},
      },
    ],
  }),
  put({table: 'issue', start: {row: {id: 1}, exclusive: false, zExtra: 1}}),
  // A row is a record, so any key is a column.
  put({table: 'issue', start: {row: {zExtra: 1}, exclusive: false}}),
  put({table: 'issue', orderBy: [['id', 'asc', 'extra']]}),
  put({table: 1}),
  [
    'changeDesiredQueries',
    {desiredQueriesPatch: [{op: 'put', hash: 'h', ast: null}]},
  ],
  ['changeDesiredQueries', {desiredQueriesPatch: [{op: 'put'}]}],
  ['pull', {clientGroupID: 'g', cookie: null, requestID: 'r'}],
  ['pull', {clientGroupID: 'g', cookie: '5', requestID: 'r'}],
  ['pull', {clientGroupID: 'g', requestID: 'r'}],
  ['updateAuth', {auth: 'x'}],
  ['updateAuth', {}],
  crud({}),
  crud({name: 'other'}),
  crud({args: []}),
  crud({args: [{ops: []}, {ops: []}]}),
  crud({args: [{}]}),
  crud({type: 'custom'}),
  [
    'push',
    {
      clientGroupID: 'g',
      mutations: [
        {
          type: 'crud',
          id: 1,
          clientID: 'c',
          name: '_zero_crud',
          args: [
            {
              ops: [
                {
                  op: 'upsert',
                  tableName: 't',
                  primaryKey: ['a', 'b'],
                  value: {a: 1, b: 2, c: null},
                },
                {
                  op: 'update',
                  tableName: 't',
                  primaryKey: ['a'],
                  value: {a: 1},
                },
                {
                  op: 'delete',
                  tableName: 't',
                  primaryKey: ['a'],
                  value: {a: 1},
                },
              ],
            },
          ],
          timestamp: 1,
        },
        {
          type: 'custom',
          id: 2,
          clientID: 'c',
          name: 'issue.create',
          args: [{id: 'a'}],
          timestamp: 2,
        },
      ],
      pushVersion: 1,
      schemaVersion: 3,
      timestamp: 3,
      requestID: 'r',
      auth: 'token',
      traceparent: 't',
    },
  ],
  [
    'push',
    {
      clientGroupID: 'g',
      mutations: [
        {
          type: 'crud',
          id: 1,
          clientID: 'c',
          name: '_zero_crud',
          args: [
            {ops: [{op: 'insert', tableName: 't', primaryKey: [], value: {}}]},
          ],
          timestamp: 1,
        },
      ],
      pushVersion: 1,
      timestamp: 1,
      requestID: 'r',
    },
  ],
  [
    'push',
    {
      clientGroupID: 'g',
      mutations: [
        {
          type: 'crud',
          id: 1,
          clientID: 'c',
          name: '_zero_crud',
          args: [
            {
              ops: [
                {
                  op: 'delete',
                  tableName: 't',
                  primaryKey: ['a'],
                  value: {a: null},
                },
              ],
            },
          ],
          timestamp: 1,
        },
      ],
      pushVersion: 1,
      timestamp: 1,
      requestID: 'r',
    },
  ],
  [
    'push',
    {
      clientGroupID: 'g',
      mutations: [
        {
          type: 'custom',
          id: 1,
          clientID: 'c',
          name: '_zero_crud',
          args: [],
          timestamp: 1,
        },
      ],
      pushVersion: 1,
      timestamp: 1,
      requestID: 'r',
    },
  ],
  ['closeConnection', []],
  ['closeConnection', [1, 'a', null, {b: []}]],
  ['closeConnection', {}],
  ['ackMutationResponses', {id: 1, clientID: 'c'}],
  ['ackMutationResponses', {id: '1', clientID: 'c'}],
  ['inspect', {id: 'i', op: 'queries'}],
  ['inspect', {id: 'i', op: 'queries', clientID: 'c'}],
  ['inspect', {id: 'i', op: 'authenticate', value: 'secret'}],
  ['inspect', {id: 'i', op: 'authenticate'}],
  ['inspect', {id: 'i', op: 'newOp'}],
  [
    'inspect',
    {
      id: 'i',
      op: 'analyze-query',
      value: asts[1],
      ast: asts[3],
      options: {vendedRows: true, syncedRows: false, joinPlans: true},
      name: 'q',
      args: [],
    },
  ],
  ['inspect', {id: 'i', op: 'analyze-query', ast: {table: 'issue', zExtra: 1}}],
  ['inspect', {id: 'i', op: 'analyze-query', options: {vendedRows: 1}}],
];

test('protocol-downstream', () => {
  const r = new Random(0xd0a1);
  emitVectors(
    'protocol-downstream',
    'protocol-downstream',
    'Downstream messages (`downstreamSchema`). `ok` is whether valita accepts the message in `passthrough` mode; `output` is its `strip` output, with each mutation result as the client reads it (`{error, message?, details?}` if it has an `error` key, else `{data?}`).',
    corpus(r, handDownstream, downstreamMessage, downstreamCase, 50, 100),
  );
  emitGeneratedVectors(
    'protocol-downstream-fuzz',
    'protocol-downstream',
    'Random and randomly edited downstream messages; see vectors/protocol-downstream.json.',
    corpus(r, [], downstreamMessage, downstreamCase, 2000, 6000),
  );
});

test('protocol-upstream', () => {
  const r = new Random(0x0b5);
  emitVectors(
    'protocol-upstream',
    'protocol-upstream',
    'Upstream messages (`upstreamSchema`, an open tagged tuple union). `ok` is whether valita accepts the message in `passthrough` mode; `output` is its `strip` output. Deeply nested messages are given as JSON text, in `messageText` and `outputText`.',
    [
      ...deepUpstream.map(m => asText(upstreamCase(m))),
      ...corpus(r, handUpstream, upstreamMessage, upstreamCase, 50, 100),
    ],
  );
  emitGeneratedVectors(
    'protocol-upstream-fuzz',
    'protocol-upstream',
    'Random and randomly edited upstream messages; see vectors/protocol-upstream.json.',
    corpus(r, [], upstreamMessage, upstreamCase, 2000, 6000),
  );
});

// Binary poke chunks.

type PokeChunksCase = {messages: string[]; ok: boolean; parts?: JSONValue};

const utf8 = new TextEncoder();

function hex(bytes: Uint8Array): string {
  return Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
}

function bytes(...parts: (string | number[])[]): Uint8Array {
  const out: number[] = [];
  for (const p of parts) {
    out.push(...(typeof p === 'string' ? utf8.encode(p) : p));
  }
  return new Uint8Array(out);
}

/** Chunk messages for `payload`, each tagged with the chunk message type. */
function chunked(payload: Uint8Array, cuts: number[]): Uint8Array[] {
  const bounds = [0, ...cuts, payload.length];
  const out: Uint8Array[] = [];
  for (let i = 1; i < bounds.length; i++) {
    out.push(
      bytes(
        [POKE_CHUNK_MESSAGE_TYPE],
        [...payload.subarray(bounds[i - 1], bounds[i])],
      ),
    );
  }
  return out;
}

function randomCuts(r: Random, length: number): number[] {
  const cuts = list(r, 6, () => r.int(length + 1));
  return cuts.sort((a, b) => a - b);
}

/**
 * The client's handling of a poke's binary messages: `#onMessage` in
 * `zero.ts` checks the type byte, then `decodePokeChunks` in
 * `zero-poke-handler.ts` decodes them.
 */
function pokeChunksCase(messages: Uint8Array[]): PokeChunksCase {
  const c: PokeChunksCase = {messages: messages.map(hex), ok: false};
  const chunks: Uint8Array[] = [];
  for (const m of messages) {
    if (m[0] !== POKE_CHUNK_MESSAGE_TYPE) {
      return c;
    }
    chunks.push(m.subarray(1));
  }
  let parsed: unknown;
  try {
    const decoder = new TextDecoder('utf-8', {fatal: true});
    let text = '';
    for (const chunk of chunks) {
      text += decoder.decode(chunk, {stream: true});
    }
    text += decoder.decode();
    parsed = JSON.parse(text);
  } catch {
    return c;
  }
  const passthrough = v.test(parsed, pokePartsSchema, 'passthrough');
  const strip = v.test(parsed, pokePartsSchema, 'strip');
  expect(strip.ok).toBe(passthrough.ok);
  if (!passthrough.ok || !strip.ok) {
    return c;
  }
  const raw = passthrough.value as Obj[];
  return {
    ...c,
    ok: true,
    parts: (strip.value as Obj[]).map((p, i) =>
      readPokePart(p, raw[i]),
    ) as JSONValue,
  };
}

function randomPokeChunks(r: Random): PokeChunksCase {
  const parts = r.bool(0.8)
    ? list(r, 3, () => pokePartBody(r))
    : list(r, 2, () => mutate(r, pokePartBody(r)));
  const payload = utf8.encode(JSON.stringify(parts));
  return pokeChunksCase(chunked(payload, randomCuts(r, payload.length)));
}

const BOM = [0xef, 0xbb, 0xbf];

const handPokeChunks: Uint8Array[][] = [
  chunked(bytes('[]'), []),
  chunked(bytes('[]'), [1]),
  chunked(bytes(' [ ] '), [0, 0, 5]),
  // An empty payload is not JSON.
  chunked(bytes(''), []),
  chunked(bytes('[{"pokeID":"中"}]'), [14, 15]),
  chunked(bytes('[{"pokeID":"\u{1f600}","zExtra":1}]'), [13, 14, 15]),
  chunked(
    bytes('[{"pokeID":"p"},{"pokeID":"q","rowsPatch":[{"op":"clear"}]}]'),
    [5, 30],
  ),
  // TextDecoder drops a leading BOM, even split across chunks, but only one.
  chunked(bytes(BOM, '[]'), []),
  chunked(bytes(BOM, '[]'), [1, 2]),
  chunked(bytes(BOM, BOM, '[]'), []),
  chunked(bytes('[', BOM, ']'), []),
  // Invalid UTF-8 is fatal.
  chunked(bytes('["', [0xff], '"]'), []),
  chunked(bytes('[{"pokeID":"', [0xc0, 0xaf], '"}]'), []),
  chunked(bytes('[{"pokeID":"', [0xed, 0xa0, 0x80], '"}]'), []),
  chunked(bytes('[{"pokeID":"', [0xf4, 0x90, 0x80, 0x80], '"}]'), []),
  chunked(bytes('[{"pokeID":"', [0xe4, 0xb8]), [13]),
  chunked(bytes('[{"pokeID":"', [0xe4, 0xb8], '"}]'), [13]),
  // Not JSON, or not PokePartBody[].
  chunked(bytes('[{"pokeID":"p"}'), []),
  chunked(bytes('{}'), []),
  chunked(bytes('[{}]'), []),
  chunked(bytes('[{"pokeID":1}]'), []),
  chunked(bytes('[null]'), []),
  // A binary message of another type, or none.
  [bytes([1], '[]')],
  [bytes('[]')],
  [bytes([0], '['), bytes([2], ']')],
  [bytes([])],
  // Mutation results are read as the client does.
  chunked(
    bytes(
      JSON.stringify([
        {
          pokeID: 'p',
          mutationsPatch: [
            {
              op: 'put',
              mutation: {
                id: mid(1),
                result: {error: 'app', message: 'm', zExtra: 1},
              },
            },
            {op: 'put', mutation: {id: mid(2), result: {data: [1], zExtra: 1}}},
          ],
        },
      ]),
    ),
    [40, 41],
  ),
];

test('protocol-poke-chunks', () => {
  const r = new Random(0xc4c);
  const cases = handPokeChunks.map(pokeChunksCase);
  for (let i = 0; i < 40; i++) {
    cases.push(randomPokeChunks(r));
  }
  emitVectors(
    'protocol-poke-chunks',
    'protocol-poke-chunks',
    'The binary poke chunks of one poke, each message in hex (a 0x00 type byte, then the next bytes of the UTF-8 JSON of a `PokePartBody[]`). `ok` is whether the client accepts them; `parts` is the `strip` output of `pokePartsSchema`, with mutation results as the client reads them (see protocol-downstream.json).',
    cases,
  );
  const fuzz: PokeChunksCase[] = [];
  for (let i = 0; i < 1500; i++) {
    fuzz.push(randomPokeChunks(r));
  }
  emitGeneratedVectors(
    'protocol-poke-chunks-fuzz',
    'protocol-poke-chunks',
    'Random poke chunks; see vectors/protocol-poke-chunks.json.',
    fuzz,
  );
});

// The Sec-WebSocket-Protocol header.

type SecProtocolsCase = {
  encoded: string;
  ok: boolean;
  decoded?: JSONValue;
  // Set for cases from `encodeSecProtocols`: the Swift encoding of the same
  // values must decode to `decoded`.
  initConnectionMessage?: JSONValue;
  authToken?: string;
};

function canonical(value: unknown): string {
  return JSON.stringify(value, (_, v: unknown) =>
    v !== null && typeof v === 'object' && !Array.isArray(v)
      ? Object.fromEntries(
          Object.entries(v as Obj).sort(([a], [b]) =>
            a < b ? -1 : a > b ? 1 : 0,
          ),
        )
      : v,
  );
}

function decodeCase(encoded: string): SecProtocolsCase {
  try {
    return {
      encoded,
      ok: true,
      decoded: decodeSecProtocols(encoded) as JSONValue,
    };
  } catch {
    return {encoded, ok: false};
  }
}

function encodeCase(
  message: JSONValue | undefined,
  authToken: string | undefined,
): SecProtocolsCase {
  if (message !== undefined) {
    // The Swift types keep every field of a valid message without extras.
    const strip = v.parse(message, initConnectionMessageSchema, 'strip');
    expect(canonical(strip)).toBe(canonical(message));
  }
  const c = decodeCase(
    encodeSecProtocols(message as InitConnectionMessage | undefined, authToken),
  );
  expect(c.ok).toBe(true);
  return {
    ...c,
    ...(message === undefined ? {} : {initConnectionMessage: message}),
    ...(authToken === undefined ? {} : {authToken}),
  };
}

const base64 = (b: Uint8Array) => btoa(String.fromCharCode(...b));

test('protocol-sec-protocols', () => {
  const r = new Random(0x5ec);
  const cases: SecProtocolsCase[] = [
    encodeCase(undefined, undefined),
    encodeCase(undefined, ''),
    encodeCase(undefined, 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig'),
    encodeCase(['initConnection', {desiredQueriesPatch: []}], undefined),
    // `encodeURIComponent` escapes `+`, `/`, and `=`; atob is forgiving.
    decodeCase('e30'),
    decodeCase('e30%3d'),
    decodeCase('e3%200%3D'),
    decodeCase(' e30= '),
    decodeCase('77u/e30='),
    decodeCase('e30=='),
    decodeCase('e3=0'),
    decodeCase('e30%'),
    decodeCase('e30%3'),
    decodeCase('%E0%A4%A'),
    decodeCase('%C3%A9'),
    decodeCase('!!!!'),
    decodeCase('a'),
    decodeCase(''),
    // TextDecoder drops a BOM and replaces invalid UTF-8.
    decodeCase(encodeURIComponent(base64(bytes(BOM, '{"authToken":"a"}')))),
    decodeCase(
      encodeURIComponent(
        base64(bytes('{"authToken":"', [0xff, 0xe4, 0xb8], '"}')),
      ),
    ),
    decodeCase(encodeURIComponent(base64(bytes('5')))),
    decodeCase(encodeURIComponent(base64(bytes('not json')))),
  ];
  const saved = extraProbability;
  extraProbability = 0;
  for (let i = 0; i < 40; i++) {
    cases.push(
      encodeCase(
        r.bool(0.9) ? ['initConnection', initConnectionBody(r)] : undefined,
        r.bool(0.8) ? str(r) : undefined,
      ),
    );
  }
  extraProbability = saved;
  emitVectors(
    'protocol-sec-protocols',
    'protocol-sec-protocols',
    'The Sec-WebSocket-Protocol value (`encodeSecProtocols` and `decodeSecProtocols`). `decoded` is what `decodeSecProtocols(encoded)` returns, if it does not throw. Cases with `initConnectionMessage` or `authToken` were encoded from them.',
    cases,
  );
});

// normalizeClientSchema.

test('protocol-client-schema', () => {
  const r = new Random(0xc5);
  const saved = extraProbability;
  extraProbability = 0;
  const cases: {schema: JSONValue; normalized: string}[] = [];
  for (let i = 0; i < 60; i++) {
    const schema = clientSchema(r);
    cases.push({
      schema,
      normalized: JSON.stringify(
        normalizeClientSchema(v.parse(schema, clientSchemaSchema)),
      ),
    });
  }
  extraProbability = saved;
  emitVectors(
    'protocol-client-schema',
    'protocol-client-schema',
    'normalizeClientSchema: tables, columns, and primary keys sorted by UTF-16 code units. `normalized` is `JSON.stringify` of the result.',
    cases,
  );
});
