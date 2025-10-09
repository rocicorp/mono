import type {LogContext} from '@rocicorp/logger';
import {unreachable} from '../../../../shared/src/asserts.ts';
import type {InspectUpBody} from '../../../../zero-protocol/src/inspect-up.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import {
  getServerVersion,
  isAdminPasswordValid,
} from '../../config/zero-config.ts';
import type {HeaderOptions} from '../../custom/fetch.ts';
import type {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {analyzeQuery} from '../analyze.ts';
import type {ClientHandler} from './client-handler.ts';
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
  headerOptions: HeaderOptions,
  userQueryURL: string | undefined,
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
          metrics: inspectorDelegate.getMetricsJSONForQuery(row.queryID),
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

        if (ast === undefined && body.name && body.args) {
          // Get the AST from the API server by transforming the named query
          ast = await inspectorDelegate.transformCustomQuery(
            body.name,
            body.args,
            headerOptions,
            userQueryURL,
          );
        }

        if (ast === undefined) {
          throw new Error(
            'AST is required for analyze-query operation. Either provide an AST directly or ensure custom query transformation is configured.',
          );
        }
        const result = await analyzeQuery(lc, config, ast, body.options);
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
    lc.error?.('Error handling inspect message', e);
    client.sendInspectResponse(lc, {
      op: 'error',
      id: body.id,
      value: (e as Error).message,
    });
  }
}
