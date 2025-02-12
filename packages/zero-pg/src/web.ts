import type {ReadonlyJSONObject} from '../../shared/src/json.ts';
import type {PushResponse} from '../../zero-protocol/src/push.ts';

export type PushHandler = (
  headers: {authorization?: string},
  body: ReadonlyJSONObject,
) => Promise<PushResponse>;
