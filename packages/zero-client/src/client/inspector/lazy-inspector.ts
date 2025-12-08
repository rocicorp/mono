import type {BTreeRead} from '../../../../replicache/src/btree/read.ts';
import type {Read} from '../../../../replicache/src/dag/store.ts';
import {readFromHash} from '../../../../replicache/src/db/read.ts';
import * as FormatVersion from '../../../../replicache/src/format-version-enum.ts';
import {getClientGroup} from '../../../../replicache/src/persist/client-groups.ts';
import {
  getClient,
  getClients,
  type ClientMap,
} from '../../../../replicache/src/persist/clients.ts';
import type {ReplicacheImpl} from '../../../../replicache/src/replicache-impl.ts';
import {withRead} from '../../../../replicache/src/with-transactions.ts';
import {assert} from '../../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import {mapValues} from '../../../../shared/src/objects.ts';
import {TDigest, type ReadonlyTDigest} from '../../../../shared/src/tdigest.ts';
import * as valita from '../../../../shared/src/valita.ts';
import type {AnalyzeQueryResult} from '../../../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {
  inspectAnalyzeQueryDownSchema,
  inspectAuthenticatedDownSchema,
  inspectMetricsDownSchema,
  inspectQueriesDownSchema,
  inspectVersionDownSchema,
  type InspectDownBody,
  type InspectQueryRow,
  type ServerMetrics as ServerMetricsJSON,
} from '../../../../zero-protocol/src/inspect-down.ts';
import type {
  AnalyzeQueryOptions,
  InspectUpBody,
} from '../../../../zero-protocol/src/inspect-up.ts';
import type {
  ClientMetricMap,
  ServerMetricMap,
} from '../../../../zql/src/query/metrics-delegate.ts';
import type {AnyQuery} from '../../../../zql/src/query/query-builder.ts';
import type {QueryDelegate} from '../../../../zql/src/query/query-delegate.ts';
import {asQueryInternals} from '../../../../zql/src/query/query-internals.ts';
import {nanoid} from '../../util/nanoid.ts';
import {ENTITIES_KEY_PREFIX} from '../keys.ts';
import type {MutatorDefs} from '../replicache-types.ts';
import {Client} from './client.ts';
import {createHTMLPasswordPrompt} from './html-dialog-prompt.ts';
import {type Lazy} from './inspector.ts';
import {Query} from './query.ts';

export type GetWebSocket = () => Promise<WebSocket>;

export type Metrics = {
  readonly [K in keyof (ClientMetricMap & ServerMetricMap)]: ReadonlyTDigest;
};

type DistributiveOmit<T, K extends string> = T extends object
  ? Omit<T, K>
  : never;

export async function rpc<T extends InspectDownBody>(
  socket: WebSocket,
  arg: DistributiveOmit<InspectUpBody, 'id'>,
  downSchema: valita.Type<T>,
): Promise<T['value']> {
  try {
    return await rpcNoAuthTry(socket, arg, downSchema);
  } catch (e) {
    if (e instanceof UnauthenticatedError) {
      const password = await createHTMLPasswordPrompt('Enter password:');
      if (password) {
        // Do authenticate rpc
        const authRes = await rpcNoAuthTry(
          socket,
          {op: 'authenticate', value: password},
          inspectAuthenticatedDownSchema,
        );
        if (authRes) {
          // If authentication is successful, retry the original RPC
          return rpcNoAuthTry(socket, arg, downSchema);
        }
      }
      throw new Error('Authentication failed');
    }
    throw e;
  }
}

function rpcNoAuthTry<T extends InspectDownBody>(
  socket: WebSocket,
  arg: DistributiveOmit<InspectUpBody, 'id'>,
  downSchema: valita.Type<T>,
): Promise<T['value']> {
  return new Promise((resolve, reject) => {
    const id = nanoid();
    const f = (ev: MessageEvent) => {
      const msg = JSON.parse(ev.data);
      if (msg[0] === 'inspect') {
        const body = msg[1];
        if (body.id !== id) {
          return;
        }
        const res = valita.test(body, downSchema);
        if (res.ok) {
          if (res.value.op === 'error') {
            reject(new Error(res.value.value));
          } else {
            resolve(res.value.value);
          }
        } else {
          // Check if we got un authenticated/false response
          const authRes = valita.test(body, inspectAuthenticatedDownSchema);
          if (authRes.ok) {
            // Handle authenticated response
            assert(
              authRes.value.value === false,
              'Expected unauthenticated response',
            );
            reject(new UnauthenticatedError());
          }

          reject(res.error);
        }
        socket.removeEventListener('message', f);
      }
    };
    socket.addEventListener('message', f);
    socket.send(JSON.stringify(['inspect', {...arg, id}]));
  });
} // T extends forces T to be resolved

export function mergeMetrics(
  clientMetrics: ClientMetrics | undefined,
  serverMetrics: ServerMetricsJSON | null | undefined,
): ClientMetrics & ServerMetrics {
  return {
    ...(clientMetrics ?? newClientMetrics()),
    ...(serverMetrics
      ? convertServerMetrics(serverMetrics)
      : newServerMetrics()),
  };
}

function newClientMetrics(): ClientMetrics {
  return {
    'query-materialization-client': new TDigest(),
    'query-materialization-end-to-end': new TDigest(),
    'query-update-client': new TDigest(),
  };
}

function newServerMetrics(): ServerMetrics {
  return {
    'query-materialization-server': new TDigest(),
    'query-update-server': new TDigest(),
  };
}

function convertServerMetrics(metrics: ServerMetricsJSON): ServerMetrics {
  return mapValues(metrics, v => TDigest.fromJSON(v));
}

export async function inspectorMetrics(
  delegate: ExtendedInspectorDelegate,
): Promise<Metrics> {
  const clientMetrics = delegate.metrics;
  const serverMetricsJSON = await rpc(
    await delegate.getSocket(),
    {op: 'metrics'},
    inspectMetricsDownSchema,
  );
  return mergeMetrics(clientMetrics, serverMetricsJSON);
}

export function inspectorClients(
  delegate: ExtendedInspectorDelegate,
): Promise<Client[]> {
  return withDagRead(delegate, dagRead => clients(delegate, dagRead));
}

export function inspectorClientsWithQueries(
  delegate: ExtendedInspectorDelegate,
): Promise<Client[]> {
  return withDagRead(delegate, dagRead =>
    clientsWithQueries(delegate, dagRead),
  );
}

async function withDagRead<T>(
  delegate: ExtendedInspectorDelegate,
  f: (dagRead: Read) => Promise<T>,
): Promise<T> {
  const {rep} = delegate;
  await rep.refresh();
  await rep.persist();
  return withRead(rep.perdag, f);
}

async function getBTree(dagRead: Read, clientID: string): Promise<BTreeRead> {
  const client = await getClient(clientID, dagRead);
  assert(client, `Client not found: ${clientID}`);
  const {clientGroupID} = client;
  const clientGroup = await getClientGroup(clientGroupID, dagRead);
  assert(clientGroup, `Client group not found: ${clientGroupID}`);
  const dbRead = await readFromHash(
    clientGroup.headHash,
    dagRead,
    FormatVersion.Latest,
  );
  return dbRead.map;
}

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
type MapEntry<T extends ReadonlyMap<any, any>> =
  T extends ReadonlyMap<infer K, infer V> ? readonly [K, V] : never;

async function clients(
  delegate: ExtendedInspectorDelegate,
  dagRead: Read,
  predicate: (entry: MapEntry<ClientMap>) => boolean = () => true,
): Promise<Client[]> {
  const clients = await getClients(dagRead);
  return [...clients.entries()]
    .filter(predicate)
    .map(
      ([clientID, {clientGroupID}]) =>
        new Client(delegate, clientID, clientGroupID),
    );
}

async function clientsWithQueries(
  delegate: ExtendedInspectorDelegate,
  dagRead: Read,
  predicate: (entry: MapEntry<ClientMap>) => boolean = () => true,
): Promise<Client[]> {
  const allClients = await clients(delegate, dagRead, predicate);
  const clientsWithQueries: Client[] = [];
  await Promise.all(
    allClients.map(async client => {
      const queries = await client.queries();
      if (queries.length > 0) {
        clientsWithQueries.push(client);
      }
    }),
  );
  return clientsWithQueries;
}

export async function clientGroupClients(
  delegate: ExtendedInspectorDelegate,
  clientGroupID: Promise<string> | string,
): Promise<Client[]> {
  const id = await clientGroupID;
  return withDagRead(delegate, dagRead =>
    clients(delegate, dagRead, ([_, v]) => v.clientGroupID === id),
  );
}

export async function clientGroupClientsWithQueries(
  delegate: ExtendedInspectorDelegate,
  clientGroupID: Promise<string> | string,
): Promise<Client[]> {
  const id = await clientGroupID;
  return withDagRead(delegate, dagRead =>
    clientsWithQueries(delegate, dagRead, ([_, v]) => v.clientGroupID === id),
  );
}

export function clientGroupQueries(
  delegate: ExtendedInspectorDelegate,
): Promise<Query[]> {
  return queries(delegate, {op: 'queries'});
}
export function clientMap(
  delegate: ExtendedInspectorDelegate,
  clientID: string,
): Promise<Map<string, ReadonlyJSONValue>> {
  return withDagRead(delegate, async dagRead => {
    const tree = await getBTree(dagRead, clientID);
    const map = new Map<string, ReadonlyJSONValue>();
    for await (const [key, value] of tree.scan('')) {
      map.set(key, value);
    }
    return map;
  });
}

export function clientRows(
  delegate: ExtendedInspectorDelegate,
  clientID: string,
  tableName: string,
): Promise<Row[]> {
  return withDagRead(delegate, async dagRead => {
    const prefix = ENTITIES_KEY_PREFIX + tableName + '/';
    const tree = await getBTree(dagRead, clientID);
    const rows: Row[] = [];
    for await (const [key, value] of tree.scan(prefix)) {
      if (!key.startsWith(prefix)) {
        break;
      }
      rows.push(value as Row);
    }
    return rows;
  });
}

export async function serverVersion(
  delegate: ExtendedInspectorDelegate,
): Promise<string> {
  return rpc(
    await delegate.getSocket(),
    {op: 'version'},
    inspectVersionDownSchema,
  );
}

export function clientQueries(
  delegate: ExtendedInspectorDelegate,
  clientID: string,
): Promise<Query[]> {
  return queries(delegate, {op: 'queries', clientID});
}

async function queries(
  delegate: ExtendedInspectorDelegate,
  arg: {op: 'queries'; clientID?: string},
): Promise<Query[]> {
  const rows: InspectQueryRow[] = await rpc(
    await delegate.getSocket(),
    arg,
    inspectQueriesDownSchema,
  );
  const queries = rows.map(row => new Query(row, delegate, delegate.getSocket));
  queries.sort((a, b) => (b.hydrateServer ?? 0) - (a.hydrateServer ?? 0));
  return queries;
}

export async function analyzeQuery(
  delegate: ExtendedInspectorDelegate,
  query: AnyQuery,
  options?: AnalyzeQueryOptions,
): Promise<AnalyzeQueryResult> {
  const qi = asQueryInternals(query);
  const {customQueryID} = qi;
  const queryParameters = customQueryID
    ? {name: customQueryID.name, args: customQueryID.args}
    : {ast: delegate.mapClientASTToServer(qi.ast)};

  return rpc(
    await delegate.getSocket(),
    {
      op: 'analyze-query',
      ...queryParameters,
      options,
    },
    inspectAnalyzeQueryDownSchema,
  );
}

class UnauthenticatedError extends Error {}

export interface InspectorDelegate {
  getQueryMetrics(hash: string): ClientMetrics | undefined;
  getAST(queryID: string): AST | undefined;
  readonly metrics: ClientMetrics;
  mapClientASTToServer(ast: AST): AST;
}

export interface ExtendedInspectorDelegate extends InspectorDelegate {
  readonly rep: Rep;
  readonly getSocket: () => Promise<WebSocket>;
  readonly queryDelegate: QueryDelegate;
  lazy: Promise<Lazy>;
}

export type Rep = ReplicacheImpl<MutatorDefs>;

export type ClientMetrics = {
  readonly [K in keyof ClientMetricMap]: ReadonlyTDigest;
};

export type ServerMetrics = {
  readonly [K in keyof ServerMetricMap]: ReadonlyTDigest;
};
