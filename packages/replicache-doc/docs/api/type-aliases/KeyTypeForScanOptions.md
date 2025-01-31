# Type Alias: KeyTypeForScanOptions\<O\>

> **KeyTypeForScanOptions**\<`O`\>: `O` *extends* [`ScanIndexOptions`](ScanIndexOptions.md) ? [`IndexKey`](IndexKey.md) : `string`

If the options contains an `indexName` then the key type is a tuple of
secondary and primary.

## Type Parameters

• **O** *extends* [`ScanOptions`](ScanOptions.md)
