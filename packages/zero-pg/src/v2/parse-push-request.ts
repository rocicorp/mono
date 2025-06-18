import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {
  pushBodySchema,
  pushParamsSchema,
  type CustomMutation,
  type MutationResponse,
  type PushBody,
} from '../../../zero-protocol/src/push.ts';
import * as v from '../../../shared/src/valita.ts';
import {
  OutOfOrderMutation,
  type Database,
  type ExtractTransactionType,
  type Params,
  type TransactionProviderHooks,
} from '../push-processor.ts';
import {MutationAlreadyProcessedError} from '../../../zero-cache/src/services/mutagen/mutagen.ts';
import {createLogContext} from '../logging.ts';
import type {LogContext, LogLevel} from '@rocicorp/logger';

type TransactFn = <D extends Database<ExtractTransactionType<D>>>(
  dbProvider: D,
  mutation: CustomMutation,
  cb: (tx: unknown, args: readonly ReadonlyJSONValue[]) => Promise<void>,
) => Promise<MutationResponse>;

export type Parsed = {
  transact: TransactFn;
  mutations: CustomMutation[];
};

export function parsePushRequest(
  queryString: URLSearchParams | Record<string, string>,
  body: ReadonlyJSONValue,
  logLevel: LogLevel,
): Promise<Parsed>;
export function parsePushRequest(
  request: Request,
  logLevel: LogLevel,
): Promise<Parsed>;
export async function parsePushRequest(
  queryOrQueryString: Request | URLSearchParams | Record<string, string>,
  body?: ReadonlyJSONValue | LogLevel,
  logLevel?: LogLevel | undefined,
): Promise<Parsed> {
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
    throw new Error('unsupportedPushVersion');
  }

  const transactor = new Transactor(req, queryParams, logLevel);
  return {
    mutations: req.mutations as CustomMutation[],
    transact: transactor.transact,
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
    cb: (
      tx: ExtractTransactionType<D>,
      args: readonly ReadonlyJSONValue[],
    ) => Promise<void>,
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

  async #transactImpl<D extends Database<ExtractTransactionType<D>>>(
    dbProvider: D,
    mutation: CustomMutation,
    cb: (
      tx: ExtractTransactionType<D>,
      args: readonly ReadonlyJSONValue[],
    ) => Promise<void>,
    errorMode: boolean,
  ): Promise<MutationResponse> {
    await dbProvider.transaction(
      async (dbTx, transactionHooks) => {
        await this.#checkAndIncrementLastMutationID(
          transactionHooks,
          mutation.clientID,
          mutation.id,
        );
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
