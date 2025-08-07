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
import {type ReadonlyTDigest} from '../../../../shared/src/tdigest.ts';
import * as valita from '../../../../shared/src/valita.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {
  inspectQueriesDownSchema,
  type InspectDownBody,
  type InspectQueryRow,
} from '../../../../zero-protocol/src/inspect-down.ts';
import type {
  InspectQueriesUpBody,
  InspectUpBody,
  InspectUpMessage,
} from '../../../../zero-protocol/src/inspect-up.ts';
import type {Schema} from '../../../../zero-schema/src/builder/schema-builder.ts';
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
  readonly 'query-materialization-client': ReadonlyTDigest;
  readonly 'query-materialization-end-to-end': ReadonlyTDigest;
};

export interface InspectorMetricsDelegate {
  getQueryMetrics(hash: string): Metrics | undefined;
  readonly metrics: Metrics;
}

export async function newInspector(
  rep: Rep,
  metricsDelegate: InspectorMetricsDelegate,
  schema: Schema,
  socket: GetWebSocket,
): Promise<InspectorInterface> {
  const clientGroupID = await rep.clientGroupID;
  return new Inspector(
    rep,
    metricsDelegate,
    schema,
    rep.clientID,
    clientGroupID,
    socket,
  );
}

class Inspector implements InspectorInterface {
  readonly #rep: Rep;
  readonly client: Client;
  readonly clientGroup: ClientGroup;
  readonly #schema: Schema;
  readonly socket: GetWebSocket;
  readonly #metricsDelegate: InspectorMetricsDelegate;

  constructor(
    rep: ReplicacheImpl,
    metricsDelegate: InspectorMetricsDelegate,
    schema: Schema,
    clientID: string,
    clientGroupID: string,
    socket: GetWebSocket,
  ) {
    this.#rep = rep;
    this.#schema = schema;
    this.client = new Client(
      rep,
      metricsDelegate,
      schema,
      socket,
      clientID,
      clientGroupID,
    );
    this.clientGroup = this.client.clientGroup;
    this.socket = socket;
    this.#metricsDelegate = metricsDelegate;
  }

  get metrics(): Metrics {
    return this.#metricsDelegate.metrics;
  }

  clients(): Promise<ClientInterface[]> {
    return withDagRead(this.#rep, dagRead =>
      clients(
        this.#rep,
        this.#metricsDelegate,
        this.socket,
        this.#schema,
        dagRead,
      ),
    );
  }

  clientsWithQueries(): Promise<ClientInterface[]> {
    return withDagRead(this.#rep, dagRead =>
      clientsWithQueries(
        this.#rep,
        this.#metricsDelegate,
        this.socket,
        this.#schema,
        dagRead,
      ),
    );
  }
}

function rpc<T extends InspectDownBody>(
  socket: WebSocket,
  arg: Omit<InspectUpBody, 'id'>,
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
          reject(res.error);
        }
        socket.removeEventListener('message', f);
      }
    };
    socket.addEventListener('message', f);
    socket.send(
      JSON.stringify(['inspect', {...arg, id}] satisfies InspectUpMessage),
    );
  });
}

class Client implements ClientInterface {
  readonly #rep: Rep;
  readonly id: string;
  readonly clientGroup: ClientGroup;
  readonly #socket: GetWebSocket;
  readonly #metricsDelegate: InspectorMetricsDelegate;

  constructor(
    rep: Rep,
    metricsDelegate: InspectorMetricsDelegate,
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
      metricsDelegate,
      socket,
      schema,
      clientGroupID,
    );
    this.#metricsDelegate = metricsDelegate;
  }

  async queries(): Promise<QueryInterface[]> {
    const rows: InspectQueryRow[] = await rpc(
      await this.#socket(),
      {op: 'queries', clientID: this.id} as InspectQueriesUpBody,
      inspectQueriesDownSchema,
    );
    return rows.map(row => new Query(row, this.#metricsDelegate));
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
  readonly #metricsDelegate: InspectorMetricsDelegate;

  constructor(
    rep: Rep,
    metricsDelegate: InspectorMetricsDelegate,
    socket: GetWebSocket,
    schema: Schema,
    id: string,
  ) {
    this.#rep = rep;
    this.#metricsDelegate = metricsDelegate;
    this.#socket = socket;
    this.#schema = schema;
    this.id = id;
  }

  clients(): Promise<ClientInterface[]> {
    return withDagRead(this.#rep, dagRead =>
      clients(
        this.#rep,
        this.#metricsDelegate,
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
        this.#metricsDelegate,
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
    return rows.map(row => new Query(row, this.#metricsDelegate));
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
  metricsDelegate: InspectorMetricsDelegate,
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
        new Client(
          rep,
          metricsDelegate,
          schema,
          socket,
          clientID,
          clientGroupID,
        ),
    );
}

async function clientsWithQueries(
  rep: Rep,
  metricsDelegate: InspectorMetricsDelegate,
  socket: GetWebSocket,
  schema: Schema,
  dagRead: Read,
  predicate: (entry: MapEntry<ClientMap>) => boolean = () => true,
): Promise<ClientInterface[]> {
  const allClients = await clients(
    rep,
    metricsDelegate,
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
  readonly ast: AST | null;
  readonly name: string | null;
  readonly args: ReadonlyArray<ReadonlyJSONValue> | null;
  readonly got: boolean;
  readonly ttl: TTL;
  readonly inactivatedAt: Date | null;
  readonly rowCount: number;
  readonly deleted: boolean;
  readonly id: string;
  readonly zql: string | null;
  readonly clientID: string;
  readonly metrics: Metrics | null;

  constructor(row: InspectQueryRow, metricsDelegate: InspectorMetricsDelegate) {
    const {ast, queryID, inactivatedAt} = row;
    // Use own properties to make this more useful in dev tools. For example, in
    // Chrome dev tools, if you do console.table(queries) you'll see the
    // properties in the table, if these were getters you would not see them in the table.
    this.clientID = row.clientID;
    this.id = queryID;
    this.inactivatedAt =
      inactivatedAt === null ? null : new Date(inactivatedAt);
    this.ttl = normalizeTTL(row.ttl);
    this.ast = ast;
    this.name = row.name;
    this.args = row.args;
    this.got = row.got;
    this.rowCount = row.rowCount;
    this.deleted = row.deleted;
    this.zql = ast ? ast.table + astToZQL(ast) : null;
    this.metrics = metricsDelegate.getQueryMetrics(queryID) ?? null;
  }
}
