import type {ReadonlyJSONValue} from 'shared/json.js';
import * as v from 'shared/valita.js';

type FunctionName = 'publish' | 'user-ensure';

const firebaseErrorResponseSchema = v.object({
  error: v.object({
    message: v.string(),
    status: v.number(),
    details: v.string().optional(),
  }),
});

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
export async function callFirebase<
  Data extends ReadonlyJSONValue,
  Return extends ReadonlyJSONValue = ReadonlyJSONValue,
>(
  functionName: FunctionName,
  data: Data,
  returnValueSchema?: v.Type<Return>,
  apiToken?: string,
): Promise<ReadonlyJSONValue> {
  // TODO(arv): Pass along auth token.
  const body = JSON.stringify({data});
  const headers = apiToken
    ? {
        'Content-type': 'application/json',
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Authorization': `Bearer ${apiToken}`,
      }
    : {
        'Content-type': 'application/json',
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

  if (!resp.ok) {
    throw new Error(`HTTP error ${resp.status}: ${resp.statusText}`);
  }

  const json = await resp.json();

  if (v.is(json, firebaseErrorResponseSchema)) {
    throw new Error(
      `Firebase error ${json.error.status}: ${json.error.message}` +
        (json.error.details ? `, ${json.error.details}` : ''),
    );
  }

  if (v.is(json, firebaseResultResponseSchema)) {
    if (returnValueSchema) {
      return v.parse(json.result, returnValueSchema);
    }

    // We know this must be JSON.
    return json.result as ReadonlyJSONValue;
  }

  throw new Error(`Unexpected response from Firebase: ${JSON.stringify(json)}`);
}
