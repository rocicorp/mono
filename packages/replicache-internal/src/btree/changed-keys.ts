import type {BTreeRead} from './read';

export async function changedKeys(
  oldMap: BTreeRead,
  newMap: BTreeRead,
): Promise<string[]> {
  const res: string[] = [];
  for await (const diffRes of newMap.diff(oldMap)) {
    res.push(diffRes.key);
  }
  return res;
}
