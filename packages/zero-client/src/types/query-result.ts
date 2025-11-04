import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';

export type QueryResultDetails =
  | {
      readonly type: 'complete';
    }
  | {
      readonly type: 'unknown';
    }
  | {
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

export type QueryErrorDetails = Extract<QueryResultDetails, {type: 'error'}>;
