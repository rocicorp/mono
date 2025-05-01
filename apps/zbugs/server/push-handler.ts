import {
  ZQLDatabase,
  PostgresJSConnection,
  PushProcessor,
} from '@rocicorp/zero/pg';
import postgres from 'postgres';
import {schema} from '../shared/schema.ts';
import {createServerMutators, type PostCommitTask} from './server-mutators.ts';
import type {AuthData} from '../shared/auth.ts';
import type {ReadonlyJSONValue} from '@rocicorp/zero';

const processor = new PushProcessor(
  new ZQLDatabase(
    new PostgresJSConnection(postgres(process.env.ZERO_UPSTREAM_DB as string)),
    schema,
  ),
);

export async function handlePush(
  authData: AuthData | undefined,
  params: Record<string, string> | URLSearchParams,
  body: ReadonlyJSONValue,
) {
  const postCommitTasks: PostCommitTask[] = [];
  const mutators = createServerMutators(authData, postCommitTasks);
  const response = await processor.process(mutators, params, body);
  await Promise.all(postCommitTasks.map(task => task()));
  return response;
}
