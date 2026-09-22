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
