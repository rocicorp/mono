import {promiseVoid} from '../resolved-promises.js';
import {
  isFirefox,
  isFirefoxPrivateBrowsingError,
} from './idb-store-with-mem-fallback.js';
import {dropMemStore} from './mem-store.js';

export function dropIDBStoreWithMemFallback(name: string): Promise<void> {
  if (!isFirefox()) {
    return dropIDBStore(name);
  }
  try {
    return dropIDBStore(name);
  } catch (e) {
    if (isFirefoxPrivateBrowsingError(e)) {
      return dropMemStore(name);
    }
  }
  return promiseVoid;
}

function dropIDBStore(name: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.deleteDatabase(name);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error);
  });
}
