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
import {assert} from '../../shared/src/asserts.ts';
import {formatPg} from '../../z2s/src/sql.ts';
import {sql} from '../../z2s/src/sql.ts';
import {MutationAlreadyProcessedError} from '../../zero-cache/src/services/mutagen/mutagen.ts';

export type PushHandler = (
  headers: Headers,
  params: Params,
  body: ReadonlyJSONObject,
) => Promise<PushResponse>;

type Headers = {
  authorization?: string | undefined;
};

type Params = {
  schema: string;
  appID: string;
};

export class PushProcessor<
  S extends Schema,
  TDBTransaction,
  MD extends CustomMutatorDefs<S, TDBTransaction>,
> {
  readonly #dbConnectionProvider: ConnectionProvider<TDBTransaction>;
  readonly #customMutatorDefs: MD;
  readonly #lc: LogContext;
  readonly #mutate: (dbTransaction: DBTransaction<unknown>) => SchemaCRUD<S>;
  readonly #query: (dbTransaction: DBTransaction<unknown>) => SchemaQuery<S>;

  constructor(
    schema: S,
    dbConnectionProvider: ConnectionProvider<TDBTransaction>,
    customMutatorDefs: MD,
  ) {
    this.#dbConnectionProvider = dbConnectionProvider;
    this.#customMutatorDefs = customMutatorDefs;
    this.#lc = createLogContext('info');
    this.#mutate = makeSchemaCRUD(schema);
    this.#query = makeSchemaQuery(schema);
  }

  async process(
    headers: Headers,
    params: Params,
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
      const res = await this.#processMutation(
        connection,
        headers,
        params,
        req,
        m,
      );
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
    params: Params,
    req: PushBody,
    m: Mutation,
  ): Promise<MutationResponse> {
    try {
      return await this.#processMutationImpl(
        dbConnection,
        headers,
        params,
        req,
        m,
        false,
      );
    } catch (e) {
      if (e instanceof OutOfOrderMutation) {
        return {
          id: {
            clientID: m.clientID,
            id: m.id,
          },
          result: {
            error: 'ooo-mutation',
            details: e.message,
          },
        };
      }

      if (e instanceof MutationAlreadyProcessedError) {
        this.#lc.warn?.(e.message);
        return {
          id: {
            clientID: m.clientID,
            id: m.id,
          },
          result: {},
        };
      }

      const ret = await this.#processMutationImpl(
        dbConnection,
        headers,
        params,
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
    params: Params,
    req: PushBody,
    m: Mutation,
    errorMode: boolean,
  ): Promise<MutationResponse> {
    if (m.type === 'crud') {
      throw new Error(
        'crud mutators are deprecated in favor of custom mutators.',
      );
    }

    return dbConnection.transaction(async (dbTx): Promise<MutationResponse> => {
      await checkAndIncrementLastMutationID(
        dbTx,
        params.schema,
        req.clientGroupID,
        m.clientID,
        m.id,
      );

      if (!errorMode) {
        await this.#dispatchMutation(dbTx, headers, m);
      }

      return {
        id: {
          clientID: m.clientID,
          id: m.id,
        },
        result: {},
      };
    });
  }

  #dispatchMutation(
    dbTx: DBTransaction<TDBTransaction>,
    headers: Headers,
    m: Mutation,
  ): Promise<void> {
    let {authorization} = headers;
    if (authorization !== undefined) {
      assert(
        authorization.toLowerCase().startsWith('bearer '),
        'Authorization header must start with `Bearer `. This is a bug in the Zero Pusher service.',
      );
      authorization = authorization.substring('Bearer '.length);
    }
    const zeroTx = new TransactionImpl(
      dbTx,
      authorization,
      m.clientID,
      m.id,
      this.#mutate(dbTx),
      this.#query(dbTx),
    );

    const [namespace, name] = splitMutatorKey(m.name);
    return this.#customMutatorDefs[namespace][name](zeroTx, m.args[0]);
  }
}

async function checkAndIncrementLastMutationID(
  tx: DBTransaction<unknown>,
  schema: string,
  clientGroupID: string,
  clientID: string,
  receivedMutationID: number,
) {
  const formatted = formatPg(
    sql`INSERT INTO ${sql.ident(schema)}.clients 
    as current ("clientGroupID", "clientID", "lastMutationID")
        VALUES (${clientGroupID}, ${clientID}, ${1})
    ON CONFLICT ("clientGroupID", "clientID")
    DO UPDATE SET "lastMutationID" = current."lastMutationID" + 1
    RETURNING "lastMutationID"`,
  );

  const [{lastMutationID}] = (await tx.query(
    formatted.text,
    formatted.values,
  )) as {lastMutationID: bigint}[];

  if (receivedMutationID < lastMutationID) {
    throw new MutationAlreadyProcessedError(
      clientID,
      receivedMutationID,
      lastMutationID,
    );
  } else if (receivedMutationID > lastMutationID) {
    throw new OutOfOrderMutation(clientID, receivedMutationID, lastMutationID);
  }
}

class OutOfOrderMutation extends Error {
  constructor(
    clientID: string,
    receivedMutationID: number,
    lastMutationID: bigint,
  ) {
    super(
      `Client ${clientID} sent mutation ID ${receivedMutationID} but expected ${lastMutationID}`,
    );
  }
}
