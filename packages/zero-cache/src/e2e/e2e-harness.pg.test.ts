import {resolver} from '@rocicorp/resolver';
import Fastify, {type FastifyInstance} from 'fastify';
import {afterEach, expect} from 'vitest';
import WebSocket from 'ws';
import {
  clearBrowserOverrides,
  overrideBrowserGlobal,
} from '../../../shared/src/browser-env.ts';
import {h128} from '../../../shared/src/hash.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {Zero} from '../../../zero-client/src/client/zero.ts';
import {
  ANYONE_CAN_DO_ANYTHING,
  definePermissions,
} from '../../../zero-permissions/src/permissions.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {
  TransformRequestMessage,
  TransformResponseMessage,
} from '../../../zero-protocol/src/custom-queries.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../../zql/src/query/named.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {getConnectionURI, test, type PgTest} from '../test/db.ts';
import {DbFile} from '../test/lite.ts';
import type {PostgresDB} from '../types/pg.ts';
import {childWorker, type Worker} from '../types/processes.ts';

const book = table('book')
  .columns({
    id: string(),
    title: string(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [book],
});

const permissions = await definePermissions(schema, () => ({
  book: ANYONE_CAN_DO_ANYTHING,
}));

const APP_ID = 'e2e';

type BookRow = {
  id: string;
  title: string;
};

function initialPGSetup() {
  return `
    CREATE TABLE book(
      id TEXT PRIMARY KEY,
      title TEXT NOT NULL
    );

    INSERT INTO book(id, title) VALUES ('1', 'Zero e2e harness');

    CREATE PUBLICATION zero_all FOR TABLE book;

    CREATE SCHEMA "${APP_ID}";

    CREATE TABLE "${APP_ID}".permissions (
      permissions JSON,
      hash TEXT
    );
    INSERT INTO "${APP_ID}".permissions (permissions, hash) VALUES ('${JSON.stringify(
      permissions,
    )}', '${h128(JSON.stringify(permissions)).toString(16)}');
  `;
}

class ZeroE2EHarness {
  readonly #apiServer: FastifyInstance;
  readonly #changeDB: PostgresDB;
  readonly #cvrDB: PostgresDB;
  readonly #port: number;
  readonly #replicaDbFile: DbFile;
  readonly #upDB: PostgresDB;
  readonly #namedQueryAST: AST;
  readonly #transformRequests: TransformRequestMessage[] = [];
  #apiURL = '';
  #zero: Worker | undefined;
  #zeroExited: Promise<number> | undefined;

  constructor(
    upDB: PostgresDB,
    cvrDB: PostgresDB,
    changeDB: PostgresDB,
    replicaDbFile: DbFile,
    port: number,
    namedQueryAST: AST,
  ) {
    this.#upDB = upDB;
    this.#cvrDB = cvrDB;
    this.#changeDB = changeDB;
    this.#replicaDbFile = replicaDbFile;
    this.#port = port;
    this.#namedQueryAST = namedQueryAST;
    this.#apiServer = Fastify();
  }

  get cacheURL() {
    return `http://localhost:${this.#port}`;
  }

  async start() {
    this.#apiServer.post('/query', async request => {
      const message = request.body as TransformRequestMessage;
      this.#transformRequests.push(message);
      const [, queries] = message;
      return [
        'transformed',
        queries.map(query => ({
          id: query.id,
          name: query.name,
          ast: this.#namedQueryAST,
        })),
      ] satisfies TransformResponseMessage;
    });
    this.#apiURL = `${await this.#apiServer.listen({port: 0})}/query`;

    const {promise: ready, resolve: onReady} = resolver<unknown>();
    const {promise: done, resolve: onClose} = resolver<number>();
    this.#zeroExited = done;

    this.#zero = childWorker(
      new URL('../server/runner/main.ts', import.meta.url),
      {
        ['ZERO_PORT']: String(this.#port),
        ['ZERO_LOG_LEVEL']: 'error',
        ['ZERO_UPSTREAM_DB']: getConnectionURI(this.#upDB),
        ['ZERO_CVR_DB']: getConnectionURI(this.#cvrDB),
        ['ZERO_CHANGE_DB']: getConnectionURI(this.#changeDB),
        ['ZERO_REPLICA_FILE']: this.#replicaDbFile.path,
        ['ZERO_APP_ID']: APP_ID,
        ['ZERO_APP_PUBLICATIONS']: 'zero_all',
        ['ZERO_NUM_SYNC_WORKERS']: '1',
        ['ZERO_ADMIN_PASSWORD']: 'e2e-admin-password',
        ['ZERO_QUERY_URL']: this.#apiURL,
      },
    );
    this.#zero.onMessageType('ready', onReady);
    this.#zero.on('close', onClose);

    await ready;
  }

  createClient(userID: string) {
    return new Zero({
      userID,
      auth: 'e2e-auth',
      schema,
      cacheURL: this.cacheURL,
      kvStore: 'mem',
    });
  }

  oracleBooks() {
    using db = new Database(
      createSilentLogContext(),
      this.#replicaDbFile.path,
      {
        readonly: true,
      },
    );
    return db
      .prepare('SELECT id, title FROM book ORDER BY id')
      .all() as BookRow[];
  }

  expectNamedQueryRouteWasUsed() {
    expect(this.#transformRequests).toContainEqual([
      'transform',
      [
        expect.objectContaining({
          name: 'allBooks',
          args: [],
        }),
      ],
    ]);
  }

  async close() {
    try {
      this.#zero?.kill('SIGTERM');
      if (this.#zeroExited) {
        expect(await this.#zeroExited).toBe(0);
      }
    } finally {
      await this.#apiServer.close();
      this.#replicaDbFile.delete();
    }
  }
}

function waitForComplete<T>(view: {
  addListener: (
    listener: (data: T, resultType: 'unknown' | 'complete' | 'error') => void,
  ) => () => void;
}) {
  return new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error('Timed out waiting for complete view result')),
      10_000,
    );
    const unsubscribe = view.addListener((data, resultType) => {
      if (resultType === 'complete') {
        clearTimeout(timeout);
        unsubscribe();
        resolve(data);
      }
    });
  });
}

function toBookRows(rows: readonly BookRow[]): BookRow[] {
  return rows.map(({id, title}) => ({id, title}));
}

let harness: ZeroE2EHarness | undefined;

afterEach(async () => {
  await harness?.close();
  harness = undefined;
  clearBrowserOverrides();
});

test('e2e harness materializes a client query and checks it against the replica', async ({
  testDBs,
}: PgTest) => {
  overrideBrowserGlobal(
    'WebSocket',
    WebSocket as unknown as typeof globalThis.WebSocket,
  );
  process.env['SINGLE_PROCESS'] = '1';

  const upDB = await testDBs.create('e2e_harness_upstream');
  const cvrDB = await testDBs.create('e2e_harness_cvr');
  const changeDB = await testDBs.create('e2e_harness_change');
  const replicaDbFile = new DbFile('e2e_harness_replica');

  await upDB.unsafe(initialPGSetup());
  const zql = createBuilder(schema);
  const bookQuery = zql.book.orderBy('id', 'asc');

  harness = new ZeroE2EHarness(
    upDB,
    cvrDB,
    changeDB,
    replicaDbFile,
    randInt(5000, 16000),
    asQueryInternals(bookQuery).ast,
  );
  await harness.start();

  const client = harness.createClient('e2e-user');
  const namedBookQuery = asQueryInternals(bookQuery).nameAndArgs(
    'allBooks',
    [],
  );
  const view = client.materialize(namedBookQuery);

  try {
    const clientRows = await waitForComplete(view);
    expect(toBookRows(clientRows)).toEqual(harness.oracleBooks());
    harness.expectNamedQueryRouteWasUsed();
  } finally {
    view.destroy();
    await client.close();
    await harness.close();
    harness = undefined;
    await testDBs.drop(upDB, cvrDB, changeDB);
  }
});
