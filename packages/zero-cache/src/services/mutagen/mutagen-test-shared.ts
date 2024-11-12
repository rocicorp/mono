export function zeroSchema(shardID: string): string {
  return /*sql*/ `
      CREATE SCHEMA zero_${shardID};
      CREATE TABLE zero_${shardID}.clients (
        "clientGroupID"  TEXT NOT NULL,
        "clientID"       TEXT NOT NULL,
        "lastMutationID" BIGINT,
        "userID"         TEXT,
        PRIMARY KEY ("clientGroupID", "clientID")
      );
      CREATE SCHEMA zero;
      CREATE TABLE zero."schemaVersions" (
        "minSupportedVersion" INT4,
        "maxSupportedVersion" INT4,

        -- Ensure that there is only a single row in the table.
        -- Application code can be agnostic to this column, and
        -- simply invoke UPDATE statements on the version columns.
        "lock" BOOL PRIMARY KEY DEFAULT true,
        CONSTRAINT zero_schema_versions_single_row_constraint CHECK (lock)
      );
      INSERT INTO zero."schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion")
        VALUES (true, 1, 1);`;
}
