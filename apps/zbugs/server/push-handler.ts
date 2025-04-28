import {
  PushProcessor,
  ZQLDatabaseProvider,
  ZQLPostgresJSAdapter,
} from '@rocicorp/zero/pg';
import postgres from 'postgres';
import {schema} from '../shared/schema.ts';
import {createServerMutators} from './server-mutators.ts';
import type {AuthData} from '../shared/auth.ts';
import type {ReadonlyJSONValue} from '@rocicorp/zero';

const processor = new PushProcessor(
  new ZQLDatabaseProvider(
    new ZQLPostgresJSAdapter(postgres(process.env.ZERO_UPSTREAM_DB as string)),
    schema,
  ),
);

export async function handlePush(
  authData: AuthData | undefined,
  params: Record<string, string> | URLSearchParams,
  body: ReadonlyJSONValue,
) {
  const mutators = createServerMutators(authData);
  const response = await processor.process(mutators, params, body);
  return response;
}
