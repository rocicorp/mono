import type {BTreeRead} from '../../../replicache/src/btree/read.ts';
import {type Read} from '../../../replicache/src/dag/store.ts';
import {readFromHash} from '../../../replicache/src/db/read.ts';
import * as FormatVersion from '../../../replicache/src/format-version-enum.ts';
import {
  getClientGroup,
  getClientGroups,
} from '../../../replicache/src/persist/client-groups.ts';
import {
  getClient,
  getClients,
  type ClientMap,
} from '../../../replicache/src/persist/clients.ts';
import type {ReplicacheImpl} from '../../../replicache/src/replicache-impl.ts';
import {withRead} from '../../../replicache/src/with-transactions.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import * as valita from '../../../shared/src/valita.ts';
import {compile} from '../../../z2s/src/compiler.ts';
import {formatPg} from '../../../z2s/src/sql.ts';
import {astSchema, type AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Format} from '../../../zql/src/ivm/view.ts';
import type {
  ClientGroup as ClientGroupInterface,
  Client as ClientInterface,
  Inspector as InspectorInterface,
  Query as QueryInterface,
} from './inspector-types.ts';
import {
  desiredQueriesPrefixForClient,
  ENTITIES_KEY_PREFIX,
  toGotQueriesKey,
} from './keys.ts';
import type {MutatorDefs} from './replicache-types.ts';

type Rep = ReplicacheImpl<MutatorDefs>;

export async function newInspector(rep: Rep): Promise<InspectorInterface> {
  const clientGroupID = await rep.clientGroupID;
  return new Inspector(rep, rep.clientID, clientGroupID);
}

class Inspector implements InspectorInterface {
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly #rep: Rep;
  readonly client: Client;
  readonly clientGroup: ClientGroup;

  constructor(rep: ReplicacheImpl, clientID: string, clientGroupID: string) {
    this.#rep = rep;
    this.clientID = clientID;
    this.clientGroupID = clientGroupID;
    this.client = new Client(rep, clientID, clientGroupID);
    this.clientGroup = this.client.clientGroup;
  }

  clients(): Promise<ClientInterface[]> {
    return withDagRead(this.#rep, dagRead => clients(this.#rep, dagRead));
  }

  clientsWithQueries(): Promise<ClientInterface[]> {
    return withDagRead(this.#rep, async dagRead => {
      const allClients = await clients(this.#rep, dagRead);
      const clientsWithQueries: ClientInterface[] = [];
      for (const client of allClients) {
        const queries = await client.queries();
        if (queries.length > 0) {
          clientsWithQueries.push(client);
        }
      }
      return clientsWithQueries;
    });
  }

  clientGroups(): Promise<ClientGroup[]> {
    return withDagRead(this.#rep, async dagRead => {
      const clientGroups = await getClientGroups(dagRead);
      return [...clientGroups.keys()].map(
        clientGroupID => new ClientGroup(this.#rep, clientGroupID),
      );
    });
  }
}

class Client implements ClientInterface {
  readonly #rep: Rep;
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly clientGroup: ClientGroup;

  constructor(rep: Rep, clientID: string, clientGroupID: string) {
    this.#rep = rep;
    this.clientID = clientID;
    this.clientGroupID = clientGroupID;
    this.clientGroup = new ClientGroup(rep, clientGroupID);
  }

  queries(): Promise<QueryInterface[]> {
    return withDagRead(this.#rep, async dagRead => {
      const prefix = desiredQueriesPrefixForClient(this.clientID);
      const tree = await getBTree(dagRead, this.clientID);
      const qs: QueryInterface[] = [];
      for await (const [key, value] of tree.scan(prefix)) {
        if (!key.startsWith(prefix)) {
          break;
        }

        const hash = key.substring(prefix.length);
        const got = await tree.has(toGotQueriesKey(hash));
        const q = new Query(hash, valita.parse(value, astSchema), got);
        qs.push(q);
      }
      return qs;
    });
  }

  map(): Promise<Map<string, ReadonlyJSONValue>> {
    return withDagRead(this.#rep, async dagRead => {
      const tree = await getBTree(dagRead, this.clientID);
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
      const tree = await getBTree(dagRead, this.clientID);
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
  readonly clientGroupID: string;

  constructor(rep: Rep, clientGroupID: string) {
    this.#rep = rep;
    this.clientGroupID = clientGroupID;
  }

  clients(): Promise<ClientInterface[]> {
    return withDagRead(this.#rep, dagRead =>
      clients(
        this.#rep,
        dagRead,
        ([_, v]) => v.clientGroupID === this.clientGroupID,
      ),
    );
  }

  queries(): Promise<QueryInterface[]> {
    throw new Error('Method not implemented.');
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
type MapEntry<T extends ReadonlyMap<any, any>> = T extends ReadonlyMap<
  infer K,
  infer V
>
  ? readonly [K, V]
  : never;

async function clients(
  rep: Rep,
  dagRead: Read,
  predicate: (entry: MapEntry<ClientMap>) => boolean = () => true,
): Promise<ClientInterface[]> {
  const clients = await getClients(dagRead);
  return [...clients.entries()]
    .filter(predicate)
    .map(
      ([clientID, {clientGroupID}]) => new Client(rep, clientID, clientGroupID),
    );
}

class Query implements QueryInterface {
  readonly id: string;
  readonly ast: AST;
  readonly got: boolean;

  constructor(id: string, ast: AST, got: boolean) {
    this.id = id;
    this.ast = ast;
    this.got = got;
  }

  get sql(): string {
    const format: Format = {
      singular: false,
      relationships: {},
    };
    const sqlQuery = formatPg(compile(this.ast, format));
    return sqlQuery.text;
  }
}
