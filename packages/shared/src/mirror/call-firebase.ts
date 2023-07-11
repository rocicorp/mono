import type {
  PublishRequest,
  PublishResponse,
} from 'mirror-protocol/src/publish.js';
import type {
  UploadRequest,
  UploadResponse,
} from 'mirror-protocol/src/reflect-server.js';
import type {
  EnsureUserRequest,
  EnsureUserResponse,
} from 'mirror-protocol/src/user.js';
import type {ReadonlyJSONValue} from '../json.js';
import * as v from '../valita.js';

// type FunctionName = 'publish' | 'user-ensure' | 'reflectServer-upload';

type CallMapping = {
  'publish': [PublishRequest, PublishResponse];
  'user-ensure': [EnsureUserRequest, EnsureUserResponse];
  'reflectServer-upload': [UploadRequest, UploadResponse];
  [name: string]: [ReadonlyJSONValue, ReadonlyJSONValue];
};

const firebaseErrorResponseSchema = v.object({
  error: v.object({
    message: v.string(),
    status: v.string(),
    details: v.string().optional(),
  }),
});

type FirebaseErrorObject = v.Infer<typeof firebaseErrorResponseSchema>['error'];

const firebaseResultResponseSchema = v.object({
  result: v.unknown(),
});

/**
 * Calls a firebase function with the given data. The data is POSTed to the
 * firebase function using the format expected by Firebase functions.
 *
 * The return value is extracted from the response and parsed using the given
 * schema.
 */
export async function callFirebase<K extends keyof CallMapping>(
  functionName: K,
  data: CallMapping[K][0],
  apiToken: string,
  returnValueSchema?: v.Type<CallMapping[K][1]>,
): Promise<CallMapping[K][1]> {
  const body = JSON.stringify({data});
  const headers = {
    'Content-type': 'application/json',
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Authorization': `Bearer ${apiToken}`,
  };

  const resp = await fetch(
    // TODO(arv): Make this a parameter/config
    `http://127.0.0.1:5001/reflect-mirror-staging/us-central1/${functionName}`,
    {
      method: 'POST',
      headers,
      body,
    },
  );

  let json: unknown;

  try {
    json = await resp.json();
  } catch (e) {
    // Even when the response is not ok, Firebase functions should send an error object.
    if (resp.ok) {
      // Not valid JSON.
      throw new Error(
        `Unexpected response from Firebase. Invalid JSON: ${
          (e as {message: unknown}).message
        }`,
      );
    }
  }

  if (json !== undefined) {
    if (v.is(json, firebaseErrorResponseSchema)) {
      throw new FirebaseError(json.error);
    }

    if (v.is(json, firebaseResultResponseSchema)) {
      if (returnValueSchema) {
        return v.parse(json.result, returnValueSchema);
      }

      // We know this must be JSON.
      return json.result as CallMapping[K][1];
    }
  } else {
    throw new Error(
      `Unexpected response from Firebase: ${resp.status}: ${resp.statusText}`,
    );
  }
  throw new Error(`Unexpected response from Firebase: ${JSON.stringify(json)}`);
}

export class FirebaseError extends Error {
  readonly name = 'FirebaseError';
  readonly status: string;
  readonly details: string | undefined;

  constructor(error: FirebaseErrorObject) {
    super(
      `${error.status}, ${error.message}${
        error.details ? `, ${error.details}` : ''
      }`,
    );
    this.status = error.status;
    this.details = error.details;
  }
}
