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
