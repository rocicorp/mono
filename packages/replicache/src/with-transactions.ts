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
