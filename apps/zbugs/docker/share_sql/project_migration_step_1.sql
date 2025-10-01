

-- create project table, add row for zero project.
DROP TABLE IF EXISTS "project";

CREATE TABLE "project" (
    "id" VARCHAR PRIMARY KEY,
    "name" VARCHAR NOT NULL
);

INSERT INTO "project" ("id", "name") VALUES ('iCNlS2qEpzYWEes1RTf-D', 'zero');


-- Add projectID column to issue and label.  
-- Populate with zero projectID. 
-- Make zero projectID default.
-- Make projectID column not nullable.

ALTER TABLE "issue"
ADD COLUMN "projectID" VARCHAR REFERENCES "project"("id");

ALTER TABLE "label"
ADD COLUMN "projectID" VARCHAR REFERENCES "project"("id");

UPDATE "issue"
SET "projectID" = 'iCNlS2qEpzYWEes1RTf-D'
WHERE "projectID" IS NULL;

UPDATE "label"
SET "projectID" = 'iCNlS2qEpzYWEes1RTf-D'
WHERE "projectID" IS NULL;


ALTER TABLE "issue"
ALTER COLUMN "projectID" SET DEFAULT 'iCNlS2qEpzYWEes1RTf-D';

ALTER TABLE "issue"
ALTER COLUMN "projectID" SET NOT NULL;

ALTER TABLE "label"
ALTER COLUMN "projectID" SET DEFAULT 'iCNlS2qEpzYWEes1RTf-D';

ALTER TABLE "label"
ALTER COLUMN "projectID" SET NOT NULL;

-- Add new indexes on issue and label that start with projectID.  Delete old indexes.

4. Add projectMember junction table.
5. Add projectMember rows making Rocicorp crew members of zero project