export function throwIfStoreClosed(store: {readonly closed: boolean}): void {
  if (store.closed) {
    throw new Error('Store is closed');
  }
}

export function throwIfTransactionClosed(transaction: {
  readonly closed: boolean;
}): void {
  if (transaction.closed) {
    throw new Error('Transaction is closed');
  }
}
export function transactionIsClosedRejection() {
  return Promise.reject(new Error('Transaction is closed'));
}

export function maybeTransactionIsClosedRejection(transaction: {
  readonly closed: boolean;
}): Promise<never> | undefined {
  return transaction.closed ? transactionIsClosedRejection() : undefined;
}
