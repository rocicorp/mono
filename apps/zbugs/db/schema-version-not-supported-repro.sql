-- Repro for SchemaVersionNotSupported: Zero schema says id is the primary key,
-- but Postgres has no primary key or non-null unique index for this table.
DROP TABLE IF EXISTS "execution_results";
CREATE TABLE "execution_results" (
  "id" text,
  "body" text
);
