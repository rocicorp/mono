import {randomUUID} from 'node:crypto';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import WebSocket, {type Data} from 'ws';
import * as v from '../../shared/src/valita.ts';
import type {AnalyzeQueryResult} from '../../zero-protocol/src/analyze-query-result.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../zero-protocol/src/client-schema.ts';
import {
  encodeSecProtocols,
  type InitConnectionMessage,
} from '../../zero-protocol/src/connect.ts';
import {inspectDownMessageSchema} from '../../zero-protocol/src/inspect-down.ts';
import {PROTOCOL_VERSION} from '../../zero-protocol/src/protocol-version.ts';

export type RemoteQuery =
  | {kind: 'ast'; ast: AST}
  | {kind: 'named'; name: string; args: ReadonlyArray<unknown>};

export type RemoteAnalyzeOptions = {
  vendedRows: boolean;
  syncedRows: boolean;
};

export async function analyzeRemote(
  lc: LogContext,
  url: string,
  adminPassword: string | undefined,
  authToken: string | undefined,
  clientSchema: ClientSchema,
  query: RemoteQuery,
  options: RemoteAnalyzeOptions,
): Promise<AnalyzeQueryResult> {
  const wsUrl = buildWsUrl(url);
  const initConnectionMessage: InitConnectionMessage = [
    'initConnection',
    {desiredQueriesPatch: [], clientSchema},
  ];
  const secProtocol = encodeSecProtocols(initConnectionMessage, authToken);

  lc.debug?.(`Connecting to ${wsUrl}`);
  const ws = new WebSocket(wsUrl, secProtocol);
  const session = new Session(lc, ws);

  try {
    await session.waitForConnected();
    await session.authenticate(adminPassword ?? '');
    return await session.analyzeQuery(query, options);
  } finally {
    session.close();
  }
}

function buildWsUrl(raw: string): string {
  const normalized = raw.replace(/^http(s?):\/\//, 'ws$1://');
  const url = new URL(normalized);
  const basePath = url.pathname.endsWith('/')
    ? url.pathname.slice(0, -1)
    : url.pathname;
  url.pathname = `${basePath}/sync/v${PROTOCOL_VERSION}/connect`;
  url.searchParams.set('clientID', randomUUID());
  url.searchParams.set('clientGroupID', randomUUID());
  url.searchParams.set('userID', '');
  url.searchParams.set('baseCookie', '');
  url.searchParams.set('ts', '0');
  url.searchParams.set('lmid', '0');
  url.searchParams.set('wsid', randomUUID());
  url.searchParams.set('profileID', '');
  return url.toString();
}

type Pending<T = unknown> = {
  op: 'authenticated' | 'analyze-query';
  resolve: (value: T) => void;
  reject: (err: Error) => void;
};

class Session {
  readonly #lc: LogContext;
  readonly #ws: WebSocket;
  readonly #connected = resolver<void>();
  readonly #pending = new Map<string, Pending>();
  #closed = false;

  constructor(lc: LogContext, ws: WebSocket) {
    this.#lc = lc;
    this.#ws = ws;
    ws.on('message', data => this.#onMessage(data));
    ws.on('error', err => this.#fail(err));
    ws.on('close', (code, reason) => {
      const text = reason.toString();
      this.#fail(
        new Error(
          `WebSocket closed${code ? ` (${code})` : ''}${text ? `: ${text}` : ''}`,
        ),
      );
    });
  }

  #fail(err: Error) {
    if (this.#closed) return;
    this.#closed = true;
    this.#connected.reject(err);
    for (const p of this.#pending.values()) {
      p.reject(err);
    }
    this.#pending.clear();
  }

  #onMessage(data: Data) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(data.toString());
    } catch (e) {
      this.#fail(new Error(`Failed to parse message as JSON: ${String(e)}`));
      return;
    }
    if (!Array.isArray(parsed) || typeof parsed[0] !== 'string') {
      return;
    }
    const [tag, body] = parsed as [string, unknown];

    if (tag === 'connected') {
      this.#connected.resolve();
      return;
    }
    if (tag === 'error') {
      this.#fail(new Error(`Server error: ${JSON.stringify(body)}`));
      return;
    }
    if (tag !== 'inspect') {
      // Ignore pokes etc. — with no desired queries we shouldn't see any, but
      // be defensive.
      return;
    }
    let down;
    try {
      down = v.parse([tag, body], inspectDownMessageSchema);
    } catch (e) {
      this.#lc.debug?.(`Ignoring unparsable inspect message: ${String(e)}`);
      return;
    }
    const [, downBody] = down;
    const pending = this.#pending.get(downBody.id);
    if (!pending) return;
    this.#pending.delete(downBody.id);
    if (downBody.op === 'error') {
      pending.reject(new Error(`Inspect error: ${downBody.value}`));
      return;
    }
    if (downBody.op !== pending.op) {
      pending.reject(
        new Error(
          `Expected inspect op '${pending.op}' for id '${downBody.id}', got '${downBody.op}'`,
        ),
      );
      return;
    }
    pending.resolve(downBody.value);
  }

  #send(msg: unknown) {
    this.#ws.send(JSON.stringify(msg));
  }

  #rpc<T>(id: string, op: Pending['op'], body: unknown): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.#pending.set(id, {
        op,
        resolve: resolve as (value: unknown) => void,
        reject,
      });
      this.#send(body);
    });
  }

  waitForConnected(): Promise<void> {
    return this.#connected.promise;
  }

  async authenticate(password: string): Promise<void> {
    const id = '1';
    const ok = await this.#rpc<boolean>(id, 'authenticated', [
      'inspect',
      {op: 'authenticate', id, value: password},
    ]);
    if (!ok) {
      throw new Error('admin password rejected');
    }
  }

  analyzeQuery(
    query: RemoteQuery,
    options: RemoteAnalyzeOptions,
  ): Promise<AnalyzeQueryResult> {
    const id = '2';
    const inner =
      query.kind === 'ast'
        ? {op: 'analyze-query', id, ast: query.ast, options}
        : {
            op: 'analyze-query',
            id,
            name: query.name,
            args: query.args,
            options,
          };
    return this.#rpc<AnalyzeQueryResult>(id, 'analyze-query', [
      'inspect',
      inner,
    ]);
  }

  close() {
    if (this.#closed) return;
    this.#closed = true;
    try {
      this.#ws.close();
    } catch {
      // ignore
    }
  }
}
