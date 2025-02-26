export function zeroSchema(appID: string, shardID: string): string {
  return /*sql*/ `
      CREATE SCHEMA ${appID}_${shardID};
      CREATE TABLE ${appID}_${shardID}.clients (
        "clientGroupID"  TEXT NOT NULL,
        "clientID"       TEXT NOT NULL,
        "lastMutationID" BIGINT,
        "userID"         TEXT,
        PRIMARY KEY ("clientGroupID", "clientID")
      );
      CREATE SCHEMA ${appID};
      CREATE TABLE ${appID}."schemaVersions" (
        "minSupportedVersion" INT4,
        "maxSupportedVersion" INT4,

        -- Ensure that there is only a single row in the table.
        -- Application code can be agnostic to this column, and
        -- simply invoke UPDATE statements on the version columns.
        "lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
      );
      INSERT INTO ${appID}."schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion")
        VALUES (true, 1, 1);`;
}
