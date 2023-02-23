export interface Release {
  release(): void;
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

export function withWrite<Write extends Release, Return>(
  store: WriteStore<Write>,
  fn: (write: Write) => Return | Promise<Return>,
): Promise<Return> {
  return using(store.write(), fn);
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
