import type {IncomingMessage} from 'node:http';
import {Agent, request} from 'node:http';
import {json as readJSON} from 'node:stream/consumers';
import type {LogContext} from '@rocicorp/logger';

/**
 * Path of the litestream control socket, derived from the replica file's
 * directory. The socket must live on a filesystem the (non-root) runtime user
 * can write — the replica directory is guaranteed writable since litestream
 * writes the replica there — so it must NOT default to a root-owned location
 * like /var/run. Kept in sync with config-v5.yml's `socket.path`, which is set
 * from `ZERO_LITESTREAM_SOCKET_PATH` (see getLitestream() in commands.ts).
 */
export function litestreamSocketPath(replicaFile: string): string {
  return `${replicaFile}.sock`;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

/** Response body from litestream's `POST /sync` control endpoint. */
export type SyncResponse = {
  /**
   * `synced` when `wait` was true and the upload completed, `synced_local`
   * when the WAL was sealed locally without waiting on the remote replica, or
   * `no_change` when there was nothing to sync.
   */
  status: 'synced' | 'synced_local' | 'no_change';
  path: string;
  txid: number;
  replicated_txid: number;
};

export type SyncOptions = {
  /**
   * When true, block until the sealed WAL has been uploaded to the remote
   * replica (litestream's SyncAndWait). When false (the default), litestream
   * only seals the WAL into local LTX files and runs its checkpoint thresholds,
   * returning without waiting on the backup store. Use false on the write path
   * where the goal is to keep the WAL small, not to durably back it up.
   */
  wait?: boolean;

  /** Overall request timeout in milliseconds. Defaults to 30s. */
  timeoutMs?: number;
};

/**
 * IPC client for the litestream control server. litestream serves an HTTP API
 * over a Unix domain socket (enabled via the `socket:` block in config-v5.yml);
 * because zero-cache runs litestream as a subprocess in the same container, the
 * socket is reachable directly on the local filesystem.
 *
 * A single keep-alive connection is held open and reused across calls, so a
 * high-frequency caller (e.g. write-path backpressure) does not pay a
 * connect/handshake per request.
 */
export class LitestreamController {
  readonly #lc: LogContext;
  readonly #replicaFile: string;
  readonly #socketPath: string;
  readonly #agent: Agent;

  /**
   * @param replicaFile Path of the replica database litestream is managing —
   *   the same value passed to litestream as `ZERO_REPLICA_FILE`. This is the
   *   `path` litestream matches control requests against.
   * @param socketPath Path of the control socket; defaults to the path derived
   *   from `replicaFile`, matching config-v5.yml's `socket.path`.
   */
  constructor(
    lc: LogContext,
    replicaFile: string,
    socketPath = litestreamSocketPath(replicaFile),
  ) {
    this.#lc = lc.withContext('component', 'litestream-controller');
    this.#replicaFile = replicaFile;
    this.#socketPath = socketPath;
    // keepAlive with a single socket keeps one warm connection to the control
    // socket that is reused across calls.
    this.#agent = new Agent({keepAlive: true, maxSockets: 1});
  }

  /**
   * Forces litestream to sync the WAL into LTX files immediately and run its
   * checkpoint thresholds, instead of waiting for the next monitor-interval
   * tick. The checkpoint itself is still litestream's usual (non-blocking)
   * PASSIVE attempt, so it is skipped if the database is busy — call this once
   * writes are quiesced.
   *
   * Best-effort: callers should tolerate rejection (e.g. ECONNREFUSED/ENOENT
   * while litestream is restarting or before it has created the socket) rather
   * than blocking application progress on it.
   */
  async sync(
    opts: SyncOptions = {},
    signal?: AbortSignal,
  ): Promise<SyncResponse> {
    const {wait = false, timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS} = opts;
    const result = await this.#post<SyncResponse>(
      '/sync',
      {path: this.#replicaFile, wait},
      timeoutMs,
      signal,
    );
    this.#lc.debug?.('litestream sync', result);
    return result;
  }

  /** Releases the pooled keep-alive connection. */
  close(): void {
    this.#agent.destroy();
  }

  // Note: Uses ye'old node:http for its unix socketPath support.
  //   fetch() would have less boiler-plate, but it would require importing
  //   the `unidici` package to provide a fetch Agent that supports sockets.
  #post<T>(
    path: string,
    body: unknown,
    timeoutMs: number,
    signal?: AbortSignal,
  ): Promise<T> {
    const payload = JSON.stringify(body);
    return new Promise<T>((resolve, reject) => {
      const req = request(
        {
          socketPath: this.#socketPath,
          method: 'POST',
          path,
          agent: this.#agent,
          timeout: timeoutMs,
          headers: {
            'content-type': 'application/json',
            'content-length': Buffer.byteLength(payload),
          },
          signal,
        },
        (res: IncomingMessage) => {
          const status = res.statusCode ?? 0;
          // litestream's success and error responses are both always JSON
          // (writeJSON / writeJSONError in server.go).
          readJSON(res).then(
            body => {
              if (status < 200 || status >= 300) {
                reject(
                  new Error(
                    `litestream ${path} failed: ${status} ${JSON.stringify(body)}`,
                  ),
                );
              } else {
                resolve(body as T);
              }
            },
            (e: unknown) =>
              reject(
                new Error(`litestream ${path} returned invalid JSON`, {
                  cause: e,
                }),
              ),
          );
        },
      );
      req.on('timeout', () =>
        req.destroy(
          new Error(`litestream ${path} timed out after ${timeoutMs}ms`),
        ),
      );
      req.on('error', reject);
      req.end(payload);
    });
  }
}
