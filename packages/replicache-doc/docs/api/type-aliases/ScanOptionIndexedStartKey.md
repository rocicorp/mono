# Type Alias: ScanOptionIndexedStartKey

> **ScanOptionIndexedStartKey**: readonly \[`string`, `string` \| `undefined`\] \| `string`

The key to start scanning at.

If you are scanning the primary index (i.e., you did not specify
`indexName`), then pass a single string for this field, which is the key in
the primary index to scan at.

If you are scanning a secondary index (i.e., you specified `indexName`), then
use the tuple form. In that case, `secondary` is the secondary key to start
scanning at, and `primary` (if any) is the primary key to start scanning at.
