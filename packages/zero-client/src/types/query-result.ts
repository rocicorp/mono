import type {Expand} from '../../../shared/src/expand.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';

export type QueryResultDetails = Expand<
  | {
      readonly type: 'complete';
    }
  | {
      readonly type: 'unknown';
    }
  | QueryErrorDetails
> & {};

export type QueryErrorDetails = {
  readonly type: 'error';
  readonly retry: () => void;
  /** @deprecated Use `retry` instead */
  readonly refetch: () => void;
  readonly error:
    | {
        readonly type: 'app';
        readonly message: string;
        readonly details?: ReadonlyJSONValue;
      }
    | {
        readonly type: 'parse';
        readonly message: string;
        readonly details?: ReadonlyJSONValue;
      };
};
