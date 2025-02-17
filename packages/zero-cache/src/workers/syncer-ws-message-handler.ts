import {trace} from '@opentelemetry/api';
import {Lock} from '@rocicorp/lock';
import type {Mutagen} from '../services/mutagen/mutagen.ts';
import type {
  SyncContext,
  TokenData,
  ViewSyncer,
} from '../services/view-syncer/view-syncer.ts';
import type {HandlerResult, MessageHandler} from './connection.ts';
import {version} from '../../../otel/src/version.ts';
import * as ErrorKind from '../../../zero-protocol/src/error-kind-enum.ts';
import type {Upstream} from '../../../zero-protocol/src/up.ts';
import {startAsyncSpan, startSpan} from '../../../otel/src/span.ts';
import {unreachable} from '../../../shared/src/asserts.ts';
import type {LogContext} from '@rocicorp/logger';
import type {JWTPayload} from 'jose';
import type {ErrorBody} from '../../../zero-protocol/src/error.ts';
import type {ConnectParams} from '../services/dispatcher/connect-params.ts';
import type {Pusher} from '../services/mutagen/pusher.ts';

const tracer = trace.getTracer('syncer-ws-server', version);

export class SyncerWsMessageHandler implements MessageHandler {
  readonly #viewSyncer: ViewSyncer;
  readonly #mutagen: Mutagen;
  readonly #mutationLock: Lock;
  readonly #lc: LogContext;
  readonly #authData: JWTPayload | undefined;
  readonly #clientGroupID: string;
  readonly #syncContext: SyncContext;
  readonly #pusher: Pusher | undefined;
  readonly #token: string | undefined;

  constructor(
    lc: LogContext,
    connectParams: ConnectParams,
    tokenData: TokenData | undefined,
    viewSyncer: ViewSyncer,
    mutagen: Mutagen,
    pusher: Pusher | undefined,
  ) {
    const {
      clientGroupID,
      clientID,
      wsID,
      baseCookie,
      protocolVersion,
      schemaVersion,
    } = connectParams;
    this.#viewSyncer = viewSyncer;
    this.#mutagen = mutagen;
    this.#mutationLock = new Lock();
    this.#lc = lc
      .withContext('connection')
      .withContext('clientID', clientID)
      .withContext('clientGroupID', clientGroupID)
      .withContext('wsID', wsID);
    this.#authData = tokenData?.decoded;
    this.#clientGroupID = clientGroupID;
    this.#pusher = pusher;
    this.#syncContext = {
      clientID,
      wsID,
      baseCookie,
      protocolVersion,
      schemaVersion,
      tokenData,
    };
  }

  async handleMessage(msg: Upstream): Promise<HandlerResult> {
    const lc = this.#lc;
    const msgType = msg[0];
    const viewSyncer = this.#viewSyncer;
    switch (msgType) {
      case 'ping':
        lc.error?.('Pull is not supported by Zero');
        break;
      case 'pull':
        lc.error?.('Pull is not supported by Zero');
        break;
      case 'push': {
        return startAsyncSpan<HandlerResult>(
          tracer,
          'connection.push',
          async () => {
            const {clientGroupID, mutations, schemaVersion} = msg[1];
            if (clientGroupID !== this.#clientGroupID) {
              return {
                type: 'fatal',
                error: {
                  kind: ErrorKind.InvalidPush,
                  message:
                    `clientGroupID in mutation "${clientGroupID}" does not match ` +
                    `clientGroupID of connection "${this.#clientGroupID}`,
                },
              } satisfies HandlerResult;
            }

            if (this.#pusher) {
              this.#pusher.enqueuePush(msg[1], this.#token);
              // We do not call mutagen since if a pusher is set
              // the precludes crud mutators.
              // We'll be removing crud mutators when we release custom mutators.
              return {type: 'ok'} satisfies HandlerResult;
            }

            // Hold a connection-level lock while processing mutations so that:
            // 1. Mutations are processed in the order in which they are received and
            // 2. A single view syncer connection cannot hog multiple upstream connections.
            const ret = await this.#mutationLock.withLock(async () => {
              const errors: ErrorBody[] = [];
              for (const mutation of mutations) {
                const maybeError = await this.#mutagen.processMutation(
                  mutation,
                  this.#authData,
                  schemaVersion,
                );
                if (maybeError !== undefined) {
                  errors.push({kind: maybeError[0], message: maybeError[1]});
                }
              }
              if (errors.length > 0) {
                return {type: 'transient', errors} satisfies HandlerResult;
              }
              return {type: 'ok'} satisfies HandlerResult;
            });
            return ret;
          },
        );
      }
      case 'changeDesiredQueries':
        await startAsyncSpan(tracer, 'connection.changeDesiredQueries', () =>
          viewSyncer.changeDesiredQueries(this.#syncContext, msg),
        );
        break;
      case 'deleteClients':
        await startAsyncSpan(tracer, 'connection.deleteClients', () =>
          viewSyncer.deleteClients(this.#syncContext, msg),
        );
        break;
      case 'initConnection': {
        // TODO (mlaw): tell mutagens about the new token too
        const stream = await startSpan(
          tracer,
          'connection.initConnection',
          () => viewSyncer.initConnection(this.#syncContext, msg),
        );
        return {
          type: 'stream',
          stream,
        };
      }
      default:
        unreachable(msgType);
    }

    return {type: 'ok'};
  }
}
