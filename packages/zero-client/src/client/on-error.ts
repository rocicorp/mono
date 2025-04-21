import {OnErrorKind} from './on-error-kind.ts';

export type OnErrorParameters = [OnErrorKind, ...unknown[]];

/**
 * A function that is called when there is an error in the Zero
 * instance. This is used to log errors to the console or
 * to a custom error handler.
 */
export type OnError = (...args: OnErrorParameters) => void;
