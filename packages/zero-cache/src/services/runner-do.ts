import Fastify, {FastifyInstance, FastifyReply, FastifyRequest} from 'fastify';
import {LogContext, LogLevel, LogSink} from '@rocicorp/logger';
import {handleConnection, Connection} from './connection.js';
import {ServiceRunner, ServiceRunnerEnv} from './service-runner.js';
import {CONNECT_URL_PATTERN, STATUS_URL_PATTERN} from './paths.js';
import websocket, {WebSocket} from '@fastify/websocket';
import type {DurableStorage} from '../storage/durable-storage.js';

export class ServiceRunnerDO {
  readonly #lc: LogContext;
  readonly #serviceRunner: ServiceRunner;
  readonly #clientConnections = new Map<string, Connection>();
  #fastify: FastifyInstance;

  constructor(
    logSink: LogSink,
    logLevel: LogLevel,
    state: DurableStorage,
    env: ServiceRunnerEnv,
  ) {
    const lc = new LogContext(logLevel, undefined, logSink).withContext(
      'component',
      'ServiceRunnerDO',
    );
    this.#serviceRunner = new ServiceRunner(lc, state, env, false);
    this.#lc = lc;
    this.#fastify = Fastify();
  }

  #connect = async (socket: WebSocket, request: FastifyRequest) => {
    try {
      handleConnection(
        this.#lc,
        this.#serviceRunner,
        this.#clientConnections,
        socket,
        request,
      );
    } catch (error) {
      this.#lc.error?.('Error in connect handler:', error);
      await socket
        .status(500)
        .send(error instanceof Error ? error.message : String(error));
    }
  };

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

  async start() {
    await this.#fastify.register(websocket);
    this.#fastify.get(CONNECT_URL_PATTERN, {websocket: true}, this.#connect);
    this.#fastify.get(STATUS_URL_PATTERN, this.#status);
    this.#fastify.listen({port: 3000}, (err, address) => {
      if (err) {
        this.#lc.error?.('Error starting server:', err);
        process.exit(1);
      }
      this.#lc.info?.(`Server listening at ${address}`);
    });
  }
}
