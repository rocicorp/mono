import {trace} from '@opentelemetry/api';
import {Lock} from '@rocicorp/lock';
import type {LogContext} from '@rocicorp/logger';
import {startAsyncSpan, startSpan} from '../../../otel/src/span.ts';
import {version} from '../../../otel/src/version.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../zero-protocol/src/error-origin.ts';
import type {ErrorBody} from '../../../zero-protocol/src/error.ts';
import type {Upstream} from '../../../zero-protocol/src/up.ts';
import type {Mutagen} from '../services/mutagen/mutagen.ts';
import type {Pusher} from '../services/mutagen/pusher.ts';
import {
  type ConnectionContextManager,
  type ConnectionSelector,
} from '../services/view-syncer/connection-context-manager.ts';
import {type ViewSyncer} from '../services/view-syncer/view-syncer.ts';
import type {ConnectParams} from './connect-params.ts';
import type {HandlerResult, MessageHandler} from './connection.ts';

const tracer = trace.getTracer('syncer-ws-server', version);

export class SyncerWsMessageHandler implements MessageHandler {
  readonly #viewSyncer: ViewSyncer;
  readonly #mutagen: Mutagen | undefined;
  readonly #mutationLock: Lock;
  readonly #lc: LogContext;
  readonly #clientGroupID: string;
  readonly #connectionSelector: ConnectionSelector;
  readonly #contextManager: ConnectionContextManager;
  readonly #pusher: Pusher | undefined;

  constructor(
    lc: LogContext,
    connectParams: ConnectParams,
    contextManager: ConnectionContextManager,
    viewSyncer: ViewSyncer,
    mutagen: Mutagen | undefined,
    pusher: Pusher | undefined,
  ) {
    const {clientGroupID, clientID, wsID} = connectParams;
    this.#viewSyncer = viewSyncer;
    this.#mutagen = mutagen;
    this.#contextManager = contextManager;
    this.#mutationLock = new Lock();
    this.#lc = lc
      .withContext('connection')
      .withContext('clientID', clientID)
      .withContext('clientGroupID', clientGroupID)
      .withContext('wsID', wsID);
    this.#clientGroupID = clientGroupID;
    this.#pusher = pusher;
    this.#connectionSelector = {
      clientID,
      wsID,
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
            const {clientGroupID, mutations} = msg[1];
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
              if (!this.#pusher) {
                return [
                  {
                    type: 'fatal',
                    error: {
                      kind: ErrorKind.InvalidPush,
                      message:
                        'A ZERO_MUTATE_URL must be set in order to process custom mutations.',
                      origin: ErrorOrigin.ZeroCache,
                    },
                  } satisfies HandlerResult,
                ];
              }
              return [
                this.#pusher.enqueuePush(this.#connectionSelector, msg[1]),
              ];
            }

            const mutagen = this.#mutagen;
            if (!mutagen) {
              return [
                {
                  type: 'fatal',
                  error: {
                    kind: ErrorKind.InvalidPush,
                    message: `Support for legacy CRUD mutations is disabled`,
                    origin: ErrorOrigin.ZeroCache,
                  },
                } satisfies HandlerResult,
              ];
            }

            const auth = this.#contextManager.mustGetConnectionContext(
              this.#connectionSelector,
            ).auth;
            assert(
              auth?.type !== 'opaque',
              'Only JWT auth is supported for CRUD mutations',
            );

            // Hold a connection-level lock while processing mutations so that:
            // 1. Mutations are processed in the order in which they are received and
            // 2. A single view syncer connection cannot hog multiple upstream connections.
            const ret = await this.#mutationLock.withLock(async () => {
              const errors: ErrorBody[] = [];
              for (const mutation of mutations) {
                const maybeError = await mutagen.processMutation(
                  mutation,
                  auth?.decoded,
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
          viewSyncer.changeDesiredQueries(this.#connectionSelector, msg),
        );
        break;
      case 'updateAuth':
        await startAsyncSpan(tracer, 'connection.updateAuth', async () => {
          const initialConnection =
            this.#contextManager.mustGetConnectionContext(
              this.#connectionSelector,
            );
          const updatedConnection = await this.#contextManager.updateAuth(
            this.#connectionSelector,
            msg[1],
          );
          const authRevisionChanged =
            updatedConnection.revision !== initialConnection.revision;

          await viewSyncer.updateAuth(
            this.#connectionSelector,
            msg,
            authRevisionChanged,
          );
        });
        break;
      case 'deleteClients': {
        const deletedClientIDs = await startAsyncSpan(
          tracer,
          'connection.deleteClients',
          () => viewSyncer.deleteClients(this.#connectionSelector, msg),
        );
        if (this.#pusher && deletedClientIDs.length > 0) {
          await this.#pusher.deleteClientMutations(
            this.#connectionSelector,
            deletedClientIDs,
          );
        }
        break;
      }
      case 'initConnection': {
        this.#contextManager.initConnection(this.#connectionSelector, msg[1]);
        const ret: HandlerResult[] = [
          {
            type: 'stream',
            source: 'viewSyncer',
            stream: startSpan(tracer, 'connection.initConnection', () =>
              viewSyncer.initConnection(this.#connectionSelector, msg),
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
            stream: this.#pusher.initConnection(this.#connectionSelector),
          });
        }

        return ret;
      }
      case 'closeConnection':
        // This message is deprecated and no longer used.
        break;

      case 'inspect':
        await startAsyncSpan(tracer, 'connection.inspect', () =>
          viewSyncer.inspect(this.#connectionSelector, msg),
        );
        break;

      case 'ackMutationResponses':
        if (this.#pusher) {
          await this.#pusher.ackMutationResponses(
            this.#connectionSelector,
            msg[1],
          );
        }
        break;

      default:
        unreachable(msgType);
    }

    return [{type: 'ok'}];
  }
}
