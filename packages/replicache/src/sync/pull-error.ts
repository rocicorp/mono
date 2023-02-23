/**
 * This error is thrown when the puller fails for any reason.
 */

export class PullError extends Error {
  name = 'PullError';
  // causedBy is used instead of cause, because while cause has been proposed as a
  // JavaScript language standard for this purpose (see
  // https://github.com/tc39/proposal-error-cause) current browser behavior is
  // inconsistent.
  causedBy?: Error | undefined;
  constructor(causedBy?: Error) {
    super('Failed to pull');
    this.causedBy = causedBy;
  }
}
