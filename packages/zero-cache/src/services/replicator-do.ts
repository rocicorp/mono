import websocket, {WebSocket} from '@fastify/websocket';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import Fastify, {FastifyInstance, FastifyReply, FastifyRequest} from 'fastify';
import {streamOut} from '../types/streams.js';
import {
  REGISTER_FILTERS_PATTERN,
  REPLICATOR_STATUS_PATTERN,
  VERSION_CHANGES_PATTERN,
} from './paths.js';
import type {RegisterInvalidationFiltersRequest} from './replicator/replicator.js';
import {ServiceRunner, ServiceRunnerEnv} from './service-runner.js';

export class ReplicatorDO {
  readonly #lc: LogContext;
  readonly #serviceRunner: ServiceRunner;
  #fastify: FastifyInstance;

  constructor(logSink: LogSink, logLevel: LogLevel, env: ServiceRunnerEnv) {
    const lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'ReplicatorDO',
    );
    this.#lc = lc;
    this.#serviceRunner = new ServiceRunner(lc, env, true);
    this.#fastify = Fastify();
  }

  async start() {
    await this.#serviceRunner.getReplicator();
    await this.#fastify.register(websocket);
    this.#initRoutes();
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
      async (
        // eslint-disable-next-line @typescript-eslint/naming-convention
        request: FastifyRequest<{Body: RegisterInvalidationFiltersRequest}>,
        reply: FastifyReply,
      ) => {
        const replicator = await this.#serviceRunner.getReplicator();
        const response = await replicator.registerInvalidationFilters(
          request.body,
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
      const status = (await this.#serviceRunner.getReplicator()).status();
      await reply.send(JSON.stringify(status));
    } catch (error) {
      this.#lc.error?.('Error in status handler:', error);
      await reply
        .status(500)
        .send(error instanceof Error ? error.message : String(error));
    }
  };

  #versionChanges = async (socket: WebSocket) => {
    const replicator = await this.#serviceRunner.getReplicator();
    const subscription = await replicator.versionChanges();

    void streamOut(
      this.#lc.withContext('stream', 'VersionChange'),
      subscription,
      socket,
    );
  };
}
