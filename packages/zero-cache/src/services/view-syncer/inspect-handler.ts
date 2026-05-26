import type {LogContext} from '@rocicorp/logger';
import {unreachable} from 'shared/src/asserts.ts';
import {must} from 'shared/src/must.ts';
import {TDigest} from 'shared/src/tdigest.ts';
import type {
  QueryServerMetrics,
  ServerMetrics,
} from 'zero-protocol/src/inspect-down.ts';
import type {InspectUpBody} from 'zero-protocol/src/inspect-up.ts';
import {Database} from 'zqlite/src/db.ts';
import {loadPermissions} from '../../auth/load-permissions.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {
  getServerVersion,
  isAdminPasswordValid,
} from '../../config/zero-config.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {analyzeQuery} from '../analyze.ts';
import type {ClientHandler} from './client-handler.ts';
import type {ConnectionContext} from './connection-context-manager.ts';
import type {CVRStore} from './cvr-store.ts';
import type {CVRSnapshot} from './cvr.ts';

export async function handleInspect(
  lc: LogContext,
  body: InspectUpBody,
  cvr: CVRSnapshot,
  client: ClientHandler,
  inspectorDelegate: InspectorDelegate,
  clientGroupID: string,
  cvrStore: CVRStore,
  config: NormalizedZeroConfig,
  ctx: ConnectionContext,
): Promise<void> {
  // Check if the client is already authenticated. We only authenticate the clientGroup
  // once per "worker".
  if (
    body.op !== 'authenticate' &&
    !inspectorDelegate.isAuthenticated(clientGroupID)
  ) {
    lc.info?.(
      'Client not authenticated to access the inspector protocol. Sending authentication challenge',
    );
    client.sendInspectResponse(lc, {
      op: 'authenticated',
      id: body.id,
      value: false,
    });
    return;
  }

  try {
    switch (body.op) {
      case 'queries': {
        const queryRows = await cvrStore.inspectQueries(
          lc,
          cvr.ttlClock,
          body.clientID,
        );

        // Enhance query rows with server-side materialization metrics
        const enhancedRows = queryRows.map(row => ({
          ...row,
          ast: row.ast ?? inspectorDelegate.getASTForQuery(row.queryID) ?? null,
          metrics: metricsForProtocol(
            inspectorDelegate.getMetricsJSONForQuery(row.queryID),
            ctx.protocolVersion,
          ),
        }));

        client.sendInspectResponse(lc, {
          op: 'queries',
          id: body.id,
          value: enhancedRows,
        });
        break;
      }

      case 'metrics': {
        client.sendInspectResponse(lc, {
          op: 'metrics',
          id: body.id,
          value: inspectorDelegate.getMetricsJSON(),
        });
        break;
      }

      case 'version':
        client.sendInspectResponse(lc, {
          op: 'version',
          id: body.id,
          value: getServerVersion(config),
        });
        break;

      case 'authenticate': {
        const password = body.value;
        const ok = isAdminPasswordValid(lc, config, password);
        if (ok) {
          inspectorDelegate.setAuthenticated(clientGroupID);
        } else {
          inspectorDelegate.clearAuthenticated(clientGroupID);
        }

        client.sendInspectResponse(lc, {
          op: 'authenticated',
          id: body.id,
          value: ok,
        });

        break;
      }

      case 'analyze-query': {
        let ast = body.ast ?? body.value;
        let legacyQuery = true;

        if (body.name && body.args) {
          // Get the AST from the API server by transforming the named query
          ast = await inspectorDelegate.transformCustomQuery(
            body.name,
            body.args,
            ctx,
          );
          legacyQuery = false;
        }

        if (ast === undefined) {
          throw new Error(
            'AST is required for analyze-query operation. Either provide an AST directly or ensure custom query transformation is configured.',
          );
        }

        let permissions;
        if (legacyQuery) {
          using db = new Database(lc, config.replica.file);
          const dbRunner = new StatementRunner(db);
          const loaded = loadPermissions(lc, dbRunner, config.app.id, config);
          if (loaded.permissions) {
            permissions = loaded.permissions;
          } else {
            lc.info?.(
              'No permissions loaded; analyze-query will run without applying permissions.',
            );
          }
        }

        const result = await analyzeQuery(
          lc,
          config,
          must(cvr.clientSchema),
          ast,
          body.options?.syncedRows,
          body.options?.vendedRows,
          permissions,
          ctx.auth?.type === 'jwt' ? ctx.auth : undefined,
          body.options?.joinPlans,
        );
        client.sendInspectResponse(lc, {
          op: 'analyze-query',
          id: body.id,
          value: result,
        });
        break;
      }

      default:
        unreachable(body);
    }
  } catch (e) {
    lc.warn?.('Error handling inspect message', e);
    client.sendInspectResponse(lc, {
      op: 'error',
      id: body.id,
      value: (e as Error).message,
    });
  }
}

/**
 * Converts per-query server metrics to the appropriate wire format based on the
 * client's protocol version.
 *
 * Protocol >= 51: new format — `query-hydration-server-ms` (plain number) +
 *   `query-update-server` (TDigest).
 * Protocol < 51: old format — `query-materialization-server` (TDigest) +
 *   `query-update-server` (TDigest). The scalar hydration time is wrapped into a
 *   one-point TDigest for backward compatibility.
 *
 * @visibleForTesting
 */
export function metricsForProtocol(
  metrics: QueryServerMetrics | null,
  protocolVersion: number,
): QueryServerMetrics | null {
  if (protocolVersion >= 51 || metrics === null) {
    return metrics;
  }
  // Backward compat: wrap the scalar hydration ms into a one-point TDigest
  // under the old field name so that 1.5 clients can parse the response.
  const hydrateDigest = new TDigest();
  const hydrateMs = metrics['query-hydration-server-ms'];
  if (hydrateMs !== undefined) {
    hydrateDigest.add(hydrateMs);
  }
  const legacyMetrics: ServerMetrics = {
    'query-materialization-server': hydrateDigest.toJSON(),
    'query-update-server': metrics['query-update-server'],
  };
  // Cast to QueryServerMetrics: this is intentional — for old-protocol clients
  // we send the legacy ServerMetrics wire shape under the QueryServerMetrics type
  // to satisfy TypeScript while preserving backward compatibility.
  return legacyMetrics as unknown as QueryServerMetrics;
}
