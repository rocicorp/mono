/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
export interface Release {
  release(): void;
}

export interface Commit {
  commit(): Promise<void>;
}

interface ReadStore<Read extends Release> {
  read(): Promise<Read>;
}

interface WriteStore<Write extends Release> {
  write(): Promise<Write>;
}

export function withRead<Read extends Release, Return>(
  store: ReadStore<Read>,
  fn: (read: Read) => Return | Promise<Return>,
): Promise<Return> {
  return using(store.read(), fn);
}

export function withWriteNoImplicitCommit<Write extends Release, Return>(
  store: WriteStore<Write>,
  fn: (write: Write) => Return | Promise<Return>,
): Promise<Return> {
  return using(store.write(), fn);
}

export function withWrite<Write extends Release & Commit, Return>(
  store: WriteStore<Write>,
  fn: (write: Write) => Return | Promise<Return>,
): Promise<Return> {
  return using(store.write(), async write => {
    const result = await fn(write);
    await write.commit();
    return result;
  });
}

/**
 * This function takes a promise for a resource and a function that uses that
 * resource. It will release the resource after the function returns by calling
 * the `release` function
 */
export async function using<TX extends Release, Return>(
  x: Promise<TX>,
  fn: (tx: TX) => Return | Promise<Return>,
): Promise<Return> {
  const write = await x;
  try {
    return await fn(write);
  } finally {
    write.release();
  }
}
