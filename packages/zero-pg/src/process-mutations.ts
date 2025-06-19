import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {
  pushBodySchema,
  pushParamsSchema,
  type CustomMutation,
  type MutationResponse,
  type PushBody,
  type PushResponse,
} from '../../zero-protocol/src/push.ts';
import * as v from '../../shared/src/valita.ts';
import {
  OutOfOrderMutation,
  type Database,
  type ExtractTransactionType,
  type Params,
  type TransactionProviderHooks,
} from './push-processor.ts';
import {MutationAlreadyProcessedError} from '../../zero-cache/src/services/mutagen/mutagen.ts';
import {createLogContext} from './logging.ts';
import type {LogContext, LogLevel} from '@rocicorp/logger';
import {assert} from '../../shared/src/asserts.ts';

export type TransactFn = <D extends Database<ExtractTransactionType<D>>>(
  dbProvider: D,
  cb: TransactFnCallback<D>,
) => Promise<MutationResponse>;

export type TransactFnCallback<D extends Database<ExtractTransactionType<D>>> =
  (
    tx: ExtractTransactionType<D>,
    mutatorName: string,
    mutatorArgs: ReadonlyJSONValue,
  ) => Promise<void>;

export type Parsed = {
  transact: TransactFn;
  mutations: CustomMutation[];
};

export function processMutations(
  cb: (
    transact: TransactFn,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  queryString: URLSearchParams | Record<string, string>,
  body: ReadonlyJSONValue,
  logLevel?: LogLevel | undefined,
): Promise<PushResponse>;
export function processMutations(
  cb: (
    transact: TransactFn,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  request: Request,
  logLevel?: LogLevel | undefined,
): Promise<PushResponse>;
export async function processMutations(
  cb: (
    transact: TransactFn,
    mutation: CustomMutation,
  ) => Promise<MutationResponse>,
  queryOrQueryString: Request | URLSearchParams | Record<string, string>,
  body?: ReadonlyJSONValue | LogLevel,
  logLevel?: LogLevel | undefined,
): Promise<PushResponse> {
  if (logLevel === undefined) {
    if (queryOrQueryString instanceof Request && typeof body === 'string') {
      logLevel = body as LogLevel;
    } else {
      logLevel = 'info';
    }
  }

  let queryString: URLSearchParams | Record<string, string>;
  if (queryOrQueryString instanceof Request) {
    const url = new URL(queryOrQueryString.url);
    queryString = url.searchParams;

    body = await queryOrQueryString.json();
  } else {
    queryString = queryOrQueryString;
  }
  const req = v.parse(body, pushBodySchema);
  if (queryString instanceof URLSearchParams) {
    queryString = Object.fromEntries(queryString);
  }
  const queryParams = v.parse(queryString, pushParamsSchema, 'passthrough');

  if (req.pushVersion !== 1) {
    return {
      error: 'unsupportedPushVersion',
    };
  }

  const transactor = new Transactor(req, queryParams, logLevel);

  const responses: MutationResponse[] = [];
  for (const m of req.mutations) {
    assert(m.type === 'custom', 'Expected custom mutation');
    const res = await cb(
      (dbProvider, innerCb) => transactor.transact(dbProvider, m, innerCb),
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

class Transactor {
  readonly #req: PushBody;
  readonly #params: Params;
  readonly #lc: LogContext;

  constructor(req: PushBody, params: Params, logLevel: LogLevel) {
    this.#req = req;
    this.#params = params;
    this.#lc = createLogContext(logLevel).withContext('PushProcessor');
  }

  transact = async <D extends Database<ExtractTransactionType<D>>>(
    dbProvider: D,
    mutation: CustomMutation,
    cb: TransactFnCallback<D>,
  ): Promise<MutationResponse> => {
    try {
      return await this.#transactImpl(dbProvider, mutation, cb, false);
    } catch (e) {
      if (e instanceof OutOfOrderMutation) {
        this.#lc.error?.(e);
        return {
          id: {
            clientID: mutation.clientID,
            id: mutation.id,
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
            clientID: mutation.clientID,
            id: mutation.id,
          },
          result: {
            error: 'alreadyProcessed',
            details: e.message,
          },
        };
      }

      const ret = await this.#transactImpl(dbProvider, mutation, cb, true);

      if ('error' in ret.result) {
        this.#lc.error?.(
          `Error ${ret.result.error} processing mutation ${mutation.id} for client ${mutation.clientID}: ${ret.result.details}`,
          e,
        );
        return ret;
      }

      this.#lc.error?.(
        `Unexpected error processing mutation ${mutation.id} for client ${mutation.clientID}`,
        e,
      );

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
  };

  #transactImpl<D extends Database<ExtractTransactionType<D>>>(
    dbProvider: D,
    mutation: CustomMutation,
    cb: TransactFnCallback<D>,
    errorMode: boolean,
  ): Promise<MutationResponse> {
    return dbProvider.transaction(
      async (dbTx, transactionHooks) => {
        await this.#checkAndIncrementLastMutationID(
          transactionHooks,
          mutation.clientID,
          mutation.id,
        );

        if (!errorMode) {
          await cb(dbTx, mutation.name, mutation.args[0]);
        }

        return {
          id: {
            clientID: mutation.clientID,
            id: mutation.id,
          },
          result: {},
        };
      },
      {
        upstreamSchema: this.#params.schema,
        clientGroupID: this.#req.clientGroupID,
        clientID: mutation.clientID,
        mutationID: mutation.id,
      },
    );
  }

  async #checkAndIncrementLastMutationID(
    transactionHooks: TransactionProviderHooks,
    clientID: string,
    receivedMutationID: number,
  ) {
    const {lastMutationID} = await transactionHooks.updateClientMutationID();

    if (receivedMutationID < lastMutationID) {
      throw new MutationAlreadyProcessedError(
        clientID,
        receivedMutationID,
        lastMutationID,
      );
    } else if (receivedMutationID > lastMutationID) {
      throw new OutOfOrderMutation(
        clientID,
        receivedMutationID,
        lastMutationID,
      );
    }
  }
}
