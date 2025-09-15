import {astToZQL} from '../../../../ast-to-zql/src/ast-to-zql.ts';
import type {BTreeRead} from '../../../../replicache/src/btree/read.ts';
import {type Read} from '../../../../replicache/src/dag/store.ts';
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
import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import type {
  ClientMetricMap,
  ServerMetricMap,
} from '../../../../zql/src/query/metrics-delegate.ts';
import {normalizeTTL, type TTL} from '../../../../zql/src/query/ttl.ts';
import {nanoid} from '../../util/nanoid.ts';
import {ENTITIES_KEY_PREFIX} from '../keys.ts';
import type {MutatorDefs} from '../replicache-types.ts';
import type {
  ClientGroup as ClientGroupInterface,
  Client as ClientInterface,
  Inspector as InspectorInterface,
  Query as QueryInterface,
} from './types.ts';

type Rep = ReplicacheImpl<MutatorDefs>;

type GetWebSocket = () => Promise<WebSocket>;

type Metrics = {
  readonly [K in keyof (ClientMetricMap & ServerMetricMap)]: ReadonlyTDigest;
};

type ClientMetrics = {
  readonly [K in keyof ClientMetricMap]: ReadonlyTDigest;
};

type ServerMetrics = {
  readonly [K in keyof ServerMetricMap]: ReadonlyTDigest;
};

export interface InspectorDelegate {
  getQueryMetrics(hash: string): ClientMetrics | undefined;
  getAST(queryID: string): AST | undefined;
  readonly metrics: ClientMetrics;
}

export async function newInspector(
  rep: Rep,
  delegate: InspectorDelegate,
  schema: Schema,
  socket: GetWebSocket,
): Promise<InspectorInterface> {
  const clientGroupID = await rep.clientGroupID;
  return new Inspector(
    rep,
    delegate,
    schema,
    rep.clientID,
    clientGroupID,
    socket,
  );
}

// T extends forces T to be resolved
type DistributiveOmit<T, K extends string> = T extends object
  ? Omit<T, K>
  : never;

class Inspector implements InspectorInterface {
  readonly #rep: Rep;
  readonly client: Client;
  readonly clientGroup: ClientGroup;
  readonly #schema: Schema;
  readonly socket: GetWebSocket;
  readonly #delegate: InspectorDelegate;

  constructor(
    rep: ReplicacheImpl,
    delegate: InspectorDelegate,
    schema: Schema,
    clientID: string,
    clientGroupID: string,
    socket: GetWebSocket,
  ) {
    this.#rep = rep;
    this.#schema = schema;
    this.client = new Client(
      rep,
      delegate,
      schema,
      socket,
      clientID,
      clientGroupID,
    );
    this.clientGroup = this.client.clientGroup;
    this.socket = socket;
    this.#delegate = delegate;
  }

  async metrics(): Promise<Metrics> {
    const clientMetrics = this.#delegate.metrics;
    const serverMetricsJSON = await rpc(
      await this.socket(),
      {op: 'metrics'},
      inspectMetricsDownSchema,
    );
    return mergeMetrics(clientMetrics, serverMetricsJSON);
  }

  clients(): Promise<ClientInterface[]> {
    return withDagRead(this.#rep, dagRead =>
      clients(this.#rep, this.#delegate, this.socket, this.#schema, dagRead),
    );
  }

  clientsWithQueries(): Promise<ClientInterface[]> {
    return withDagRead(this.#rep, dagRead =>
      clientsWithQueries(
        this.#rep,
        this.#delegate,
        this.socket,
        this.#schema,
        dagRead,
      ),
    );
  }

  async serverVersion(): Promise<string> {
    return rpc(await this.socket(), {op: 'version'}, inspectVersionDownSchema);
  }
}

class UnauthenticatedError extends Error {}

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
          resolve(res.value.value);
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
}

async function rpc<T extends InspectDownBody>(
  socket: WebSocket,
  arg: DistributiveOmit<InspectUpBody, 'id'>,
  downSchema: valita.Type<T>,
): Promise<T['value']> {
  try {
    return await rpcNoAuthTry(socket, arg, downSchema);
  } catch (e) {
    if (e instanceof UnauthenticatedError) {
      const password = prompt('Enter password:');
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

class Client implements ClientInterface {
  readonly #rep: Rep;
  readonly id: string;
  readonly clientGroup: ClientGroup;
  readonly #socket: GetWebSocket;
  readonly #delegate: InspectorDelegate;

  constructor(
    rep: Rep,
    delegate: InspectorDelegate,
    schema: Schema,
    socket: GetWebSocket,
    id: string,
    clientGroupID: string,
  ) {
    this.#rep = rep;
    this.#socket = socket;
    this.id = id;
    this.clientGroup = new ClientGroup(
      rep,
      delegate,
      socket,
      schema,
      clientGroupID,
    );
    this.#delegate = delegate;
  }

  async queries(): Promise<QueryInterface[]> {
    const rows: InspectQueryRow[] = await rpc(
      await this.#socket(),
      {op: 'queries', clientID: this.id},
      inspectQueriesDownSchema,
    );
    return rows.map(row => new Query(row, this.#delegate, this.#socket));
  }

  map(): Promise<Map<string, ReadonlyJSONValue>> {
    return withDagRead(this.#rep, async dagRead => {
      const tree = await getBTree(dagRead, this.id);
      const map = new Map<string, ReadonlyJSONValue>();
      for await (const [key, value] of tree.scan('')) {
        map.set(key, value);
      }
      return map;
    });
  }

  rows(tableName: string): Promise<Row[]> {
    return withDagRead(this.#rep, async dagRead => {
      const prefix = ENTITIES_KEY_PREFIX + tableName;
      const tree = await getBTree(dagRead, this.id);
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
}

class ClientGroup implements ClientGroupInterface {
  readonly #rep: Rep;
  readonly id: string;
  readonly #schema: Schema;
  readonly #socket: GetWebSocket;
  readonly #delegate: InspectorDelegate;

  constructor(
    rep: Rep,
    delegate: InspectorDelegate,
    socket: GetWebSocket,
    schema: Schema,
    id: string,
  ) {
    this.#rep = rep;
    this.#delegate = delegate;
    this.#socket = socket;
    this.#schema = schema;
    this.id = id;
  }

  clients(): Promise<ClientInterface[]> {
    return withDagRead(this.#rep, dagRead =>
      clients(
        this.#rep,
        this.#delegate,
        this.#socket,
        this.#schema,
        dagRead,
        ([_, v]) => v.clientGroupID === this.id,
      ),
    );
  }

  clientsWithQueries(): Promise<ClientInterface[]> {
    return withDagRead(this.#rep, dagRead =>
      clientsWithQueries(
        this.#rep,
        this.#delegate,
        this.#socket,
        this.#schema,
        dagRead,
        ([_, v]) => v.clientGroupID === this.id,
      ),
    );
  }

  async queries(): Promise<QueryInterface[]> {
    const rows: InspectQueryRow[] = await rpc(
      await this.#socket(),
      {op: 'queries'},
      inspectQueriesDownSchema,
    );
    return rows.map(row => new Query(row, this.#delegate, this.#socket));
  }
}

async function withDagRead<T>(
  rep: Rep,
  f: (dagRead: Read) => Promise<T>,
): Promise<T> {
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type MapEntry<T extends ReadonlyMap<any, any>> =
  T extends ReadonlyMap<infer K, infer V> ? readonly [K, V] : never;

async function clients(
  rep: Rep,
  delegate: InspectorDelegate,
  socket: GetWebSocket,
  schema: Schema,
  dagRead: Read,
  predicate: (entry: MapEntry<ClientMap>) => boolean = () => true,
): Promise<ClientInterface[]> {
  const clients = await getClients(dagRead);
  return [...clients.entries()]
    .filter(predicate)
    .map(
      ([clientID, {clientGroupID}]) =>
        new Client(rep, delegate, schema, socket, clientID, clientGroupID),
    );
}

async function clientsWithQueries(
  rep: Rep,
  delegate: InspectorDelegate,
  socket: GetWebSocket,
  schema: Schema,
  dagRead: Read,
  predicate: (entry: MapEntry<ClientMap>) => boolean = () => true,
): Promise<ClientInterface[]> {
  const allClients = await clients(
    rep,
    delegate,
    socket,
    schema,
    dagRead,
    predicate,
  );
  const clientsWithQueries: ClientInterface[] = [];
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

class Query implements QueryInterface {
  readonly #socket: GetWebSocket;

  readonly name: string | null;
  readonly args: ReadonlyArray<ReadonlyJSONValue> | null;
  readonly got: boolean;
  readonly ttl: TTL;
  readonly inactivatedAt: Date | null;
  readonly rowCount: number;
  readonly deleted: boolean;
  readonly id: string;
  readonly clientID: string;
  readonly metrics: Metrics | null;
  readonly clientZQL: string | null;
  readonly serverZQL: string | null;
  readonly #serverAST: AST | null;

  constructor(
    row: InspectQueryRow,
    delegate: InspectorDelegate,
    socket: GetWebSocket,
  ) {
    this.#socket = socket;

    const {ast, queryID, inactivatedAt} = row;
    // Use own properties to make this more useful in dev tools. For example, in
    // Chrome dev tools, if you do console.table(queries) you'll see the
    // properties in the table, if these were getters you would not see them in the table.
    this.clientID = row.clientID;
    this.id = queryID;
    this.inactivatedAt =
      inactivatedAt === null ? null : new Date(inactivatedAt);
    this.ttl = normalizeTTL(row.ttl);
    this.name = row.name;
    this.args = row.args;
    this.got = row.got;
    this.rowCount = row.rowCount;
    this.deleted = row.deleted;
    this.#serverAST = ast;
    this.serverZQL = ast ? ast.table + astToZQL(ast) : null;
    const clientAST = delegate.getAST(queryID);
    this.clientZQL = clientAST ? clientAST.table + astToZQL(clientAST) : null;

    // Merge client and server metrics
    const clientMetrics = delegate.getQueryMetrics(queryID);
    const serverMetrics = row.metrics;

    this.metrics = mergeMetrics(clientMetrics, serverMetrics);
  }

  async analyze(options?: AnalyzeQueryOptions): Promise<AnalyzeQueryResult> {
    assert(this.#serverAST, 'No server AST available for this query');
    return rpc(
      await this.#socket(),
      {
        op: 'analyze-query',
        value: this.#serverAST,
        options,
      },
      inspectAnalyzeQueryDownSchema,
    );
  }
}

function mergeMetrics(
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
