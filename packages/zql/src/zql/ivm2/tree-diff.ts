import type {Row} from './data.js';

/**
 * Because of subqueries, the output of Zero queries are trees, not sets like in
 * a normal database. Thus, fundamentally, the output of IVM must represent
 * changes to a *tree*.
 *
 * Because we care about minimizing memory consumption on the server, TreeDiff
 * is lazy. You don’t get the entire tree of changes at once. Instead you
 * consume a TreeDiff by iterating it. As each Change comes out of a level of
 * TreeDiff, you gain access to "subdiffs" that represents the changes to each
 * subquery. These in turn are also lazy.
 */
export type TreeDiff = Iterable<Change>;

/**
 * Currently, the only change types are "add" and "remove". We represent edits
 * as a remove followed by an add. We expect to be able to add "edit" type
 * changes in the future.
 *
 * We need "nop" because we need to represent the situation where a child
 * TreeDiff has a change, but its parent does not.
 */
export type ChangeType = 'nop' | 'add' | 'remove';

/**
 * A single change to a parent row, along with any subdiffs. The "names" of
 * subdiffs come from the alias the user specifies for a subquery. We actually
 * don't require aliases at this level, and could use an array of subdiffs
 * instead. But the names seem useful for debugability.
 */
export type Change = {
  readonly type: ChangeType;
  readonly row: Row;
  readonly subdiffs?: Map<string, TreeDiff> | undefined;
};
