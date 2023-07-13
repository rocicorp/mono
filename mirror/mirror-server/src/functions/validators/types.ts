import type {CallableRequest} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';

export type AsyncCallable<Request, Response> = (
  request: CallableRequest<Request>,
) => Promise<Response>;

export type AsyncHandler<Request, Response> = (
  request: Request,
  context: CallableRequest<Request>,
) => Promise<Response>;

export type AsyncHandlerWithAuth<Request, Response> = (
  request: Request,
  context: CallableRequestWithAuth<Request>,
) => Promise<Response>;

export interface CallableRequestWithAuth<Request>
  extends CallableRequest<Request> {
  auth: AuthData;
}
