/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/await-thenable, @typescript-eslint/no-misused-promises, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-floating-promises, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/require-await, @typescript-eslint/no-empty-object-type, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error */
/**
 * Callback function invoked when an error occurs within a Zero instance.
 *
 * @param message - A descriptive error message explaining what went wrong
 * @param rest - Additional context or error details. These are typically:
 *   - Error objects with stack traces
 *   - JSON-serializable data related to the error context
 *   - State information at the time of the error
 */
export type OnError = (message: string, ...rest: unknown[]) => void;

/**
 * Type representing the parameter types of the {@link OnError} callback.
 */
export type OnErrorParameters = Parameters<OnError>;
