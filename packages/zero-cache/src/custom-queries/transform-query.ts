import type {TransformedAndHashed} from '../auth/read-authorizer.ts';
import type {CustomQueryRecord} from '../services/view-syncer/schema/types.ts';
import {
  transformResponseMessageSchema,
  type ErroredQuery,
  type TransformRequestBody,
  type TransformRequestMessage,
} from '../../../zero-protocol/src/custom-queries.ts';
import {fetchFromAPIServer, type HeaderOptions} from '../custom/fetch.ts';
import type {ShardID} from '../types/shards.ts';
import * as v from '../../../shared/src/valita.ts';

type HttpError = {
  error: 'http';
  status: number;
  details: string;
};

export async function transformCustomQueries(
  pullUrl: string,
  shard: ShardID,
  headerOptions: HeaderOptions,
  queries: CustomQueryRecord[],
): Promise<(TransformedAndHashed | ErroredQuery)[] | HttpError> {
  const request: TransformRequestBody = queries.map(query => ({
    id: query.id,
    name: query.name,
    args: query.args,
  }));

  const response = await fetchFromAPIServer(
    pullUrl,
    shard,
    headerOptions,
    undefined,
    ['transform', request] satisfies TransformRequestMessage,
  );

  if (!response.ok) {
    return {
      error: 'http',
      status: response.status,
      details: await response.text(),
    };
  }

  const body = await response.json();
  const msg = v.parse(body, transformResponseMessageSchema);

  return msg[1].map(transformed => {
    if ('error' in transformed) {
      return transformed;
    }
    return {
      query: transformed.ast,
      hash: transformed.id,
    } satisfies TransformedAndHashed;
  });
}
