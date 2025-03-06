import type {ReadonlyJSONObject} from '../../shared/src/json.ts';
import type {
  Mutation,
  MutationResponse,
  PushBody,
  PushResponse,
} from '../../zero-protocol/src/push.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import * as v from '../../shared/src/valita.ts';
import {pushBodySchema} from '../../zero-protocol/src/push.ts';
import {
  makeSchemaCRUD,
  TransactionImpl,
  type CustomMutatorDefs,
} from './custom.ts';
import {first} from '../../shared/src/iterables.ts';
import {LogContext} from '@rocicorp/logger';
import {createLogContext} from './logging.ts';
import {
  splitMutatorKey,
  type SchemaCRUD,
  type SchemaQuery,
  type ConnectionProvider,
  type DBConnection,
  type DBTransaction,
} from '../../zql/src/mutate/custom.ts';
import {makeSchemaQuery} from './query.ts';

export type PushHandler = (
  headers: Headers,
  body: ReadonlyJSONObject,
) => Promise<PushResponse>;

type Headers = {authorization?: string | undefined};

function pgZeroSchema(shardID: string) {
  return `zero_${shardID}`;
}

export class PushProcessor<
  S extends Schema,
  TDBTransaction,
  MD extends CustomMutatorDefs<S, TDBTransaction>,
> {
  readonly #dbConnectionProvider: ConnectionProvider<TDBTransaction>;
  readonly #customMutatorDefs: MD;
  readonly #lc: LogContext;
  readonly #shardID: string;
  readonly #mutate: (dbTransaction: DBTransaction<unknown>) => SchemaCRUD<S>;
  readonly #query: (dbTransaction: DBTransaction<unknown>) => SchemaQuery<S>;

  constructor(
    shardID: string,
    schema: S,
    dbConnectionProvider: ConnectionProvider<TDBTransaction>,
    customMutatorDefs: MD,
  ) {
    this.#shardID = shardID;
    this.#dbConnectionProvider = dbConnectionProvider;
    this.#customMutatorDefs = customMutatorDefs;
    this.#lc = createLogContext('info');
    this.#mutate = makeSchemaCRUD(schema);
    this.#query = makeSchemaQuery(schema);
  }

  async process(
    headers: Headers,
    body: ReadonlyJSONObject,
  ): Promise<PushResponse> {
    const req = v.parse(body, pushBodySchema);
    const connection = await this.#dbConnectionProvider();

    if (req.pushVersion !== 1) {
      return {
        error: 'unsupported-push-version',
      };
    }

    const responses: MutationResponse[] = [];
    for (const m of req.mutations) {
      const res = await this.#processMutation(connection, headers, req, m);
      responses.push(res);
      if ('error' in res.result) {
        break;
      }
    }

    return {
      mutations: responses,
    };
  }

  async #processMutation(
    dbConnection: DBConnection<TDBTransaction>,
    headers: Headers,
    req: PushBody,
    m: Mutation,
  ): Promise<MutationResponse> {
    try {
      return await this.#processMutationImpl(
        dbConnection,
        headers,
        req,
        m,
        false,
      );
    } catch (e) {
      const ret = await this.#processMutationImpl(
        dbConnection,
        headers,
        req,
        m,
        true,
      );
      if ('error' in ret.result) {
        return ret;
      }
      return {
        id: ret.id,
        result: {
          error: 'app',
          details:
            e instanceof Error
              ? e.message
              : 'exception was not of type `Error`',
        },
      };
    }
  }

  #processMutationImpl(
    dbConnection: DBConnection<TDBTransaction>,
    headers: Headers,
    req: PushBody,
    m: Mutation,
    errorMode: boolean,
  ): Promise<MutationResponse> {
    if (m.type === 'crud') {
      throw new Error(
        'crud mutators are deprecated in favor of custom mutators.',
      );
    }
    const pgSchema = pgZeroSchema(this.#shardID);

    return dbConnection.transaction(async (dbTx): Promise<MutationResponse> => {
      const id = {
        clientID: m.clientID,
        id: m.id,
      } as const;
      const client = first(
        await dbTx.query(
          `select * from "${pgSchema}"."clients" where "clientID" = $1`,
          [m.clientID],
        ),
      ) as {lastMutationID: bigint} | undefined;
      const lmid = client?.lastMutationID ?? 0n;
      const expected = lmid + 1n;

      if (m.id < expected) {
        this.#lc.warn?.(`Mutation ${m.id} already processed. Skipping.`);
        return {id, result: {}};
      }
      if (m.id > expected) {
        // if m.id is 1, then client can interpret this like client-not-found
        // and nuke client state.
        return {
          id,
          result: {
            error: 'ooo-mutation',
          },
        };
      }

      if (!errorMode) {
        await this.#dispatchMutation(dbTx, headers, m);
      }

      if (expected === 1n) {
        await dbTx.query(
          `insert into "${pgSchema}"."clients" ("clientGroupID", "clientID", "lastMutationID") values ($1, $2, $3)`,
          [req.clientGroupID, m.clientID, expected],
        );
      } else {
        await dbTx.query(
          `update "${pgSchema}"."clients" set "lastMutationID" = $1 where "clientID" = $2 and "clientGroupID" = $3`,
          [expected, m.clientID, req.clientGroupID],
        );
      }

      return {
        id,
        result: {},
      };
    });
  }

  #dispatchMutation(
    dbTx: DBTransaction<TDBTransaction>,
    headers: Headers,
    m: Mutation,
  ): Promise<void> {
    const zeroTx = new TransactionImpl(
      dbTx,
      headers.authorization,
      m.clientID,
      m.id,
      this.#mutate(dbTx),
      this.#query(dbTx),
    );

    const [namespace, name] = splitMutatorKey(m.name);
    return this.#customMutatorDefs[namespace][name](zeroTx, m.args);
  }
}
