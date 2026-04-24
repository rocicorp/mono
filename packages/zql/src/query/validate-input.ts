import type {StandardSchemaV1} from '@standard-schema/spec';

export class InputValidationError extends Error {
  readonly result: StandardSchemaV1.FailureResult;

  // will get picked up by `getErrorDetails` when mapping to an `ApplicationError`
  readonly details: {
    type: 'InputValidationError';
    result: StandardSchemaV1.FailureResult;
  };

  constructor(message: string, result: StandardSchemaV1.FailureResult) {
    super(message);
    this.name = 'InputValidationError';
    this.result = result;
    this.details = {type: 'InputValidationError', result};
  }
}

/**
 * Validates input using a StandardSchema validator if provided.
 * This is shared validation logic used by both defineQuery and defineMutator.
 *
 * @param name - The name of the query or mutator (for error messages)
 * @param input - The input value to validate
 * @param validator - Optional StandardSchema validator
 * @param kind - Type of definition ('query' or 'mutator') for error messages
 * @returns The validated output (either transformed by validator or input as-is)
 * @throws Error if validation fails or if an async validator is used
 * @internal
 */
export function validateInput<TInput, TOutput>(
  name: string,
  input: TInput,
  validator: StandardSchemaV1<TInput, TOutput> | undefined,
  kind: 'query' | 'mutator',
): TOutput {
  if (!validator) {
    // No validator, so input and output are the same
    return input as unknown as TOutput;
  }

  const result = validator['~standard'].validate(input);
  if (result instanceof Promise) {
    throw new Error(
      `Async validators are not supported. ${titleCase(kind)} name ${name}`,
    );
  }
  if (result.issues) {
    throw new InputValidationError(
      `Validation failed for ${kind} ${name}: ${result.issues
        .map(issue => issue.message)
        .join(', ')}`,
      result,
    );
  }
  return result.value;
}

function titleCase(kind: string): string {
  return kind.charAt(0).toUpperCase() + kind.slice(1);
}
