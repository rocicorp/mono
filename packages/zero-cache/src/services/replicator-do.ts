import Fastify, {FastifyInstance, FastifyReply, FastifyRequest} from 'fastify';
import websocket, {WebSocket} from '@fastify/websocket';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {streamOut} from '../types/streams.js';
import {
  REGISTER_FILTERS_PATTERN,
  REPLICATOR_STATUS_PATTERN,
  VERSION_CHANGES_PATTERN,
} from './paths.js';
import type {RegisterInvalidationFiltersRequest} from './replicator/replicator.js';
import {ServiceRunner, ServiceRunnerEnv} from './service-runner.js';
import type {DurableStorage} from '../storage/durable-storage.js';
export class ReplicatorDO {
  readonly #lc: LogContext;
  readonly #serviceRunner: ServiceRunner;
  #fastify: FastifyInstance;

  constructor(
    logSink: LogSink,
    logLevel: LogLevel,
    storage: DurableStorage,
    env: ServiceRunnerEnv,
  ) {
    const lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'ReplicatorDO',
    );
    this.#lc = lc;
    this.#serviceRunner = new ServiceRunner(lc, storage, env, true);
    this.#fastify = Fastify();
    void (async () => {
      this.#fastify = Fastify();
      await this.#fastify.register(websocket);
    })();

    this.#initRoutes();
  }

  start() {
    this.#fastify.listen({port: 3001}, (err, address) => {
      if (err) {
        this.#lc.error?.('Error starting server:', err);
        process.exit(1);
      }
      this.#lc.info?.(`Server listening at ${address}`);
    });
  }

  #initRoutes() {
    this.#fastify.post(REPLICATOR_STATUS_PATTERN, this.#status);
    this.#fastify.post(
      REGISTER_FILTERS_PATTERN,
      async (request: FastifyRequest, reply: FastifyReply) => {
        const replicator = await this.#serviceRunner.getReplicator();
        const response = await replicator.registerInvalidationFilters(
          request.body as RegisterInvalidationFiltersRequest, //this needs to validate and not cast
        );
        await reply.send(response);
      },
    );
    this.#fastify.get(
      VERSION_CHANGES_PATTERN,
      {websocket: true},
      this.#versionChanges,
    );
  }

  #status = async (_request: FastifyRequest, reply: FastifyReply) => {
    try {
      const status = await this.#serviceRunner.status();
      await reply.send(JSON.stringify(status));
    } catch (error) {
      this.#lc.error?.('Error in status handler:', error);
      await reply
        .status(500)
        .send(error instanceof Error ? error.message : String(error));
    }
  };

  #versionChanges = async (socket: WebSocket, request: FastifyRequest) => {
    if (request.headers['upgrade'] !== 'websocket') {
      this.#lc.info?.('Missing Upgrade header for', request.url);
      return new Response('expected WebSocket Upgrade header', {status: 400});
    }

    const replicator = await this.#serviceRunner.getReplicator();
    const subscription = await replicator.versionChanges();

    void streamOut(
      this.#lc.withContext('stream', 'VersionChange'),
      subscription,
      socket,
    );

    // Sec-WebSocket-Protocol is used as a mechanism for sending `auth`
    // since custom headers are not supported by the browser WebSocket API, the
    // Sec-WebSocket-Protocol semantics must be followed. Send a
    // Sec-WebSocket-Protocol response header with a value matching the
    // Sec-WebSocket-Protocol request header, to indicate support for the
    // protocol, otherwise the client will close the connection.
    const responseHeaders = new Headers();
    const protocol = request.headers['sec-websocket-protocol'];
    if (protocol) {
      socket.setProtocol(protocol);
    }
    return {
      status: 101,
      webSocket: socket,
      headers: responseHeaders,
    };
  };
}
