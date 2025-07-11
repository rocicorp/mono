import {PushProcessor} from '@rocicorp/zero/server';
import {zeroPostgresJS} from '@rocicorp/zero/server/adapters/postgresjs';
import {schema} from '../shared/schema.ts';
import {createServerMutators, type PostCommitTask} from './server-mutators.ts';
import type {AuthData} from '../shared/auth.ts';
import type {ReadonlyJSONValue} from '@rocicorp/zero';

const processor = new PushProcessor(
  zeroPostgresJS(schema, process.env.ZERO_UPSTREAM_DB as string),
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
