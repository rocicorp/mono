/* eslint-disable @typescript-eslint/no-explicit-any */
import BTree_ from 'sorted-btree';
// eslint-disable-next-line @typescript-eslint/naming-convention
const BTree =
  ((BTree_ as any).default as typeof BTree_) || (BTree_ as typeof BTree_);

export default BTree;
