import {PushProcessor, type Params} from '@rocicorp/zero/pg';
import postgres from 'postgres';
import {schema} from '../shared/schema.ts';
import {createServerMutators, type PostCommitTask} from './server-mutators.ts';
import type {ReadonlyJSONObject} from '@rocicorp/zero';
import type {AuthData} from '../shared/auth.ts';
import {connectionProvider} from '@rocicorp/zero/pg';

const provider = connectionProvider(
  postgres(process.env.ZERO_UPSTREAM_DB as string),
);

export async function handlePush(
  authData: AuthData | undefined,
  params: Params,
  body: ReadonlyJSONObject,
) {
  const postCommitTasks: PostCommitTask[] = [];
  const mutators = createServerMutators(authData, postCommitTasks);
  const processor = new PushProcessor(schema, provider, mutators);
  const response = await processor.process(params, body);
  await Promise.all(postCommitTasks.map(task => task()));
  return response;
}
