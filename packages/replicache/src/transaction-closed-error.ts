/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
/**
 * This error is thrown when you try to call methods on a closed transaction.
 */
export class TransactionClosedError extends Error {
  constructor() {
    super('Transaction is closed');
  }
}

export type Closed = {closed: boolean};

export function throwIfClosed(tx: Closed): void {
  if (tx.closed) {
    throw new TransactionClosedError();
  }
}

export function rejectIfClosed(tx: Closed): undefined | Promise<never> {
  return tx.closed ? Promise.reject(new TransactionClosedError()) : undefined;
}
