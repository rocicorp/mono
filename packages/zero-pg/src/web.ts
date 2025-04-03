import {
  type Mutation,
  type MutationResponse,
  type PushBody,
  pushBodySchema,
  pushParamsSchema,
  type PushResponse,
} from '../../zero-protocol/src/push.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import * as v from '../../shared/src/valita.ts';
import {
  makeSchemaCRUD,
  TransactionImpl,
  type CustomMutatorDefs,
} from './custom.ts';
import {LogContext, type LogLevel} from '@rocicorp/logger';
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
import {formatPg} from '../../z2s/src/sql.ts';
import {sql} from '../../z2s/src/sql.ts';
import {MutationAlreadyProcessedError} from '../../zero-cache/src/services/mutagen/mutagen.ts';
import type {ServerSchema} from '../../z2s/src/schema.ts';
import {getServerSchema} from './schema.ts';
import {assert} from '../../shared/src/asserts.ts';

export type Params = v.Infer<typeof pushParamsSchema>;

export class PushProcessor<
  S extends Schema,
  TDBTransaction,
  MD extends CustomMutatorDefs<S, TDBTransaction>,
> {
  readonly #dbConnectionProvider: ConnectionProvider<TDBTransaction>;
  readonly #lc: LogContext;
  readonly #mutate: (
    dbTransaction: DBTransaction<unknown>,
    serverSchema: ServerSchema,
  ) => SchemaCRUD<S>;
  readonly #query: (
    dbTransaction: DBTransaction<unknown>,
    serverSchema: ServerSchema,
  ) => SchemaQuery<S>;
  readonly #schema: S;

  constructor(
    schema: S,
    dbConnectionProvider: ConnectionProvider<TDBTransaction>,
    logLevel: LogLevel = 'info',
  ) {
    this.#dbConnectionProvider = dbConnectionProvider;
    this.#lc = createLogContext(logLevel).withContext('PushProcessor');
    this.#mutate = makeSchemaCRUD(schema);
    this.#query = makeSchemaQuery(schema);
    this.#schema = schema;
  }

  async process(
    mutators: MD,
    queryString: unknown,
    body: unknown,
  ): Promise<PushResponse> {
    const req = v.parse(body, pushBodySchema);
    const params = v.parse(queryString, pushParamsSchema);
    const connection = await this.#dbConnectionProvider();

    if (req.pushVersion !== 1) {
      this.#lc.error?.(
        `Unsupported push version ${req.pushVersion} for clientGroupID ${req.clientGroupID}`,
      );
      return {
        error: 'unsupportedPushVersion',
      };
    }

    const responses: MutationResponse[] = [];
    for (const m of req.mutations) {
      const res = await this.#processMutation(
        connection,
        mutators,
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
    mutators: MD,
    params: Params,
    req: PushBody,
    m: Mutation,
  ): Promise<MutationResponse> {
    try {
      return await this.#processMutationImpl(
        dbConnection,
        mutators,
        params,
        req,
        m,
        false,
      );
    } catch (e) {
      if (e instanceof OutOfOrderMutation) {
        this.#lc.error?.(e);
        return {
          id: {
            clientID: m.clientID,
            id: m.id,
          },
          result: {
            error: 'oooMutation',
            details: e.message,
          },
        };
      }

      if (e instanceof MutationAlreadyProcessedError) {
        this.#lc.warn?.(e);
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
        mutators,
        params,
        req,
        m,
        true,
      );
      if ('error' in ret.result) {
        this.#lc.error?.(
          `Error ${ret.result.error} processing mutation ${m.id} for client ${m.clientID}: ${ret.result.details}`,
        );
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
    mutators: MD,
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
        this.#lc,
        dbTx,
        params.schema,
        req.clientGroupID,
        m.clientID,
        m.id,
      );

      if (!errorMode) {
        const serverSchema = await getServerSchema(dbTx, this.#schema);
        await this.#dispatchMutation(dbTx, serverSchema, mutators, m);
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
    serverSchema: ServerSchema,
    mutators: MD,
    m: Mutation,
  ): Promise<void> {
    const zeroTx = new TransactionImpl(
      dbTx,
      m.clientID,
      m.id,
      this.#mutate(dbTx, serverSchema),
      this.#query(dbTx, serverSchema),
    );

    const [namespace, name] = splitMutatorKey(m.name);
    if (name === undefined) {
      const mutator = mutators[namespace];
      assert(
        typeof mutator === 'function',
        () => `could not find mutator ${m.name}`,
      );
      return mutator(zeroTx, m.args[0]);
    }

    const mutatorGroup = mutators[namespace];
    assert(
      typeof mutatorGroup === 'object',
      () => `could not find mutators for namespace ${namespace}`,
    );
    const mutator = mutatorGroup[name];
    assert(
      typeof mutator === 'function',
      () => `could not find mutator ${m.name}`,
    );
    return mutator(zeroTx, m.args[0]);
  }
}

async function checkAndIncrementLastMutationID(
  lc: LogContext,
  tx: DBTransaction<unknown>,
  schema: string,
  clientGroupID: string,
  clientID: string,
  receivedMutationID: number,
) {
  lc.debug?.(`Incrementing LMID. Received: ${receivedMutationID}`);
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
  lc.debug?.(
    `Incremented LMID. Received: ${receivedMutationID}. New: ${lastMutationID}`,
  );
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
