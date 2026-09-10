/**
 * Kept in a module that is not re-exported by the public change protocol so
 * internal consumers can share the tag list without expanding the public API.
 */
export const schemaChangeTags = [
  'create-table',
  'rename-table',
  'update-table-metadata',
  'add-column',
  'update-column',
  'drop-column',
  'drop-table',
  'create-index',
  'drop-index',
  'backfill-completed',
] as const;

/**
 * Backfill control tags: messages that annotate backfill runs without
 * carrying rows or DDL. Like {@link schemaChangeTags}, this is a closed list
 * used to tag-filter reads of the change log.
 */
export const backfillControlTags = ['backfill-started'] as const;
