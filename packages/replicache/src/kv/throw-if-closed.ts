function storeError(): Error {
  return new Error('Store is closed');
}

function transactionError(): Error {
  return new Error('Transaction is closed');
}

export function throwIfStoreClosed(store: {readonly closed: boolean}): void {
  if (store.closed) {
    throw storeError();
  }
}

export function throwIfTransactionClosed(transaction: {
  readonly closed: boolean;
}): void {
  if (transaction.closed) {
    throw transactionError();
  }
}

export function transactionIsClosedRejection() {
  return Promise.reject(transactionError());
}

export function maybeTransactionIsClosedRejection(transaction: {
  readonly closed: boolean;
}): Promise<never> | undefined {
  return transaction.closed ? transactionIsClosedRejection() : undefined;
}

export function storeIsClosedRejection() {
  return Promise.reject(storeError());
}
