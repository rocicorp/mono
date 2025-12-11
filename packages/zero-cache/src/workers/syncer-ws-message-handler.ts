import {trace} from '@opentelemetry/api';
import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import type {JWTPayload} from 'jose';
import {startAsyncSpan, startSpan} from '../../../otel/src/span.ts';
import {version} from '../../../otel/src/version.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import type {ErrorBody} from '../../../zero-protocol/src/error.ts';
import type {Upstream} from '../../../zero-protocol/src/up.ts';
import type {Mutagen} from '../services/mutagen/mutagen.ts';
import type {Pusher} from '../services/mutagen/pusher.ts';
import type {
  SyncContext,
  TokenData,
  ViewSyncer,
} from '../services/view-syncer/view-syncer.ts';
import type {ConnectParams} from './connect-params.ts';
import type {HandlerResult, MessageHandler} from './connection.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';

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
  // DEPRECATED: remove #token
  // and forward auth and cookie headers that were
  // sent with the push.
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
      httpCookie,
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
    this.#token = tokenData?.raw;
    this.#clientGroupID = clientGroupID;
    this.#pusher = pusher;
    this.#syncContext = {
      clientID,
      wsID,
      baseCookie,
      protocolVersion,
      schemaVersion,
      tokenData,
      httpCookie,
    };
  }

  async handleMessage(msg: Upstream): Promise<HandlerResult[]> {
    const lc = this.#lc;
    const msgType = msg[0];
    const viewSyncer = this.#viewSyncer;
    switch (msgType) {
      case 'ping':
        lc.error?.('Ping is not supported at this layer by Zero');
        break;
      case 'pull':
        lc.error?.('Pull is not supported by Zero');
        break;
      case 'push': {
        return startAsyncSpan<HandlerResult[]>(
          tracer,
          'connection.push',
          async () => {
            const {clientGroupID, mutations, schemaVersion} = msg[1];
            if (clientGroupID !== this.#clientGroupID) {
              return [
                {
                  type: 'fatal',
                  error: {
                    kind: ErrorKind.InvalidPush,
                    message:
                      `clientGroupID in mutation "${clientGroupID}" does not match ` +
                      `clientGroupID of connection "${this.#clientGroupID}`,
                    origin: ErrorOrigin.ZeroCache,
                  },
                } satisfies HandlerResult,
              ];
            }

            if (mutations.length === 0) {
              return [
                {
                  type: 'ok',
                },
              ];
            }

            // The client only ever sends 1 mutation per push.
            // #pusher will throw if it sees a CRUD mutation.
            // #mutagen will throw if it see a custom mutation.
            if (mutations[0].type === 'custom') {
              assert(
                this.#pusher,
                'A ZERO_MUTATE_URL must be set in order to process custom mutations.',
              );
              return [
                this.#pusher.enqueuePush(
                  this.#syncContext.clientID,
                  msg[1],
                  this.#token,
                  this.#syncContext.httpCookie,
                ),
              ];
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
                  this.#pusher !== undefined,
                );
                if (maybeError !== undefined) {
                  errors.push({
                    kind: maybeError[0],
                    message: maybeError[1],
                    origin: ErrorOrigin.ZeroCache,
                  });
                }
              }
              if (errors.length > 0) {
                return {type: 'transient', errors} satisfies HandlerResult;
              }
              return {type: 'ok'} satisfies HandlerResult;
            });
            return [ret];
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
        const ret: HandlerResult[] = [
          {
            type: 'stream',
            source: 'viewSyncer',
            stream: startSpan(tracer, 'connection.initConnection', () =>
              viewSyncer.initConnection(this.#syncContext, msg),
            ),
          },
        ];

        // Given we support both CRUD and Custom mutators,
        // we do not initialize the `pusher` unless the user has opted
        // into custom mutations. We detect that by checking
        // if the pushURL has been set.
        if (this.#pusher) {
          ret.push({
            type: 'stream',
            source: 'pusher',
            stream: this.#pusher.initConnection(
              this.#syncContext.clientID,
              this.#syncContext.wsID,
              msg[1].userPushURL,
            ),
          });
        }

        return ret;
      }
      case 'closeConnection':
        // This message is deprecated and no longer used.
        break;

      case 'inspect':
        await startAsyncSpan(tracer, 'connection.inspect', () =>
          viewSyncer.inspect(this.#syncContext, msg),
        );
        break;

      case 'ackMutationResponses':
        if (this.#pusher) {
          await this.#pusher.ackMutationResponses(msg[1]);
        }
        break;

      default:
        unreachable(msgType);
    }

    return [{type: 'ok'}];
  }
}
