import {jsonSchema} from './json-schema.ts';
import type {ReadonlyJSONValue} from './json.ts';

export function getErrorMessage(error: unknown): string {
  return getErrorMessageInternal(error, new Set());
}

function getErrorMessageInternal(error: unknown, seen: Set<unknown>): string {
  if (typeof error === 'string') {
    return error;
  }

  if (typeof error === 'object' && error !== null) {
    if (seen.has(error)) {
      return 'Circular error reference detected while extracting the error message.';
    }
    seen.add(error);
  }

  if (error instanceof Error) {
    if (error.message) {
      return error.message;
    }

    if ('cause' in error) {
      const {cause} = error as {cause: unknown};
      if (cause !== undefined) {
        const causeMessage = getErrorMessageInternal(cause, seen);
        if (causeMessage) {
          return causeMessage;
        }
      }
    }
  }

  if (
    typeof error === 'object' &&
    error !== null &&
    'message' in error &&
    typeof (error as {message: unknown}).message === 'string'
  ) {
    const message = (error as {message: string}).message;
    if (message) {
      return message;
    }
  }

  try {
    const json = jsonSchema.parse(error);
    return `Parsed message: ${JSON.stringify(json)}`;
  } catch {}

  return `Unknown error of type ${typeof error} was thrown and the message could not be determined. See cause for details.`;
}

export function getErrorDetails(error: unknown): ReadonlyJSONValue | undefined {
  if (error instanceof Error) {
    if ('details' in error) {
      try {
        return jsonSchema.parse(error?.details);
      } catch {}
    }

    if (error.name && error.name !== 'Error') {
      return {name: error.name};
    }

    return undefined;
  }

  if (typeof error === 'object' && error !== null && 'details' in error) {
    try {
      return jsonSchema.parse((error as {details: ReadonlyJSONValue})?.details);
    } catch {}
  }

  try {
    return jsonSchema.parse(error);
  } catch {}

  return undefined;
}
