# Type Alias: ExperimentalDiffOperation\<Key\>

> **ExperimentalDiffOperation**\<`Key`\>: [`ExperimentalDiffOperationAdd`](ExperimentalDiffOperationAdd.md)\<`Key`\> \| [`ExperimentalDiffOperationDel`](ExperimentalDiffOperationDel.md)\<`Key`\> \| [`ExperimentalDiffOperationChange`](ExperimentalDiffOperationChange.md)\<`Key`\>

**`Experimental`**

The individual parts describing the changes that happened to the Replicache
data. There are three different kinds of operations:
- `add`: A new entry was added.
- `del`: An entry was deleted.
- `change`: An entry was changed.

 This type is experimental and may change in the future.

## Type Parameters

• **Key**
