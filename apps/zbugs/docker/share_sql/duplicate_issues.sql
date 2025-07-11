-- SQL script to duplicate all issues with their comments and labels
-- Each duplicated record gets a new GUID
-- Useful for generating test data to debug perf with that is smaller
-- than i.e., docker-compose-1gb.yml
BEGIN;

-- Create a temporary table to map old issue IDs to new issue IDs
CREATE TEMP TABLE issue_id_mapping AS
SELECT
  id as old_issue_id,
  gen_random_uuid() :: varchar as new_issue_id
FROM
  issue;

-- 1. Duplicate all issues with new GUIDs
INSERT INTO
  issue (
    id,
    title,
    "open",
    modified,
    created,
    "creatorID",
    "assigneeID",
    description,
    visibility,
    "testJson"
  )
SELECT
  mapping.new_issue_id,
  i.title,
  i."open",
  EXTRACT(
    EPOCH
    FROM
      CURRENT_TIMESTAMP
  ) * 1000,
  -- Set current timestamp for modified
  EXTRACT(
    EPOCH
    FROM
      CURRENT_TIMESTAMP
  ) * 1000,
  -- Set current timestamp for created
  i."creatorID",
  i."assigneeID",
  i.description,
  i.visibility,
  i."testJson"
FROM
  issue i
  JOIN issue_id_mapping mapping ON i.id = mapping.old_issue_id;

-- 2. Duplicate all comments with new GUIDs and correct issue references
INSERT INTO
  comment (
    id,
    "issueID",
    created,
    body,
    "creatorID"
  )
SELECT
  gen_random_uuid() :: varchar,
  mapping.new_issue_id,
  EXTRACT(
    EPOCH
    FROM
      CURRENT_TIMESTAMP
  ) * 1000,
  -- Set current timestamp for created
  c.body,
  c."creatorID"
FROM
  comment c
  JOIN issue_id_mapping mapping ON c."issueID" = mapping.old_issue_id;

-- 3. Duplicate all issue labels with correct issue references
INSERT INTO
  "issueLabel" ("labelID", "issueID")
SELECT
  il."labelID",
  mapping.new_issue_id
FROM
  "issueLabel" il
  JOIN issue_id_mapping mapping ON il."issueID" = mapping.old_issue_id;

-- 4. Duplicate viewState records for the new issues
INSERT INTO
  "viewState" ("userID", "issueID", viewed)
SELECT
  vs."userID",
  mapping.new_issue_id,
  vs.viewed
FROM
  "viewState" vs
  JOIN issue_id_mapping mapping ON vs."issueID" = mapping.old_issue_id;

-- 5. Duplicate emoji records for issues (not comments)
INSERT INTO
  emoji (
    id,
    value,
    annotation,
    "subjectID",
    "creatorID",
    created
  )
SELECT
  gen_random_uuid() :: varchar,
  e.value,
  e.annotation,
  mapping.new_issue_id,
  e."creatorID",
  EXTRACT(
    EPOCH
    FROM
      CURRENT_TIMESTAMP
  ) * 1000
FROM
  emoji e
  JOIN issue_id_mapping mapping ON e."subjectID" = mapping.old_issue_id;

-- Display summary of what was duplicated
SELECT
  'Issues duplicated' as operation,
  COUNT(*) as count
FROM
  issue_id_mapping
UNION
ALL
SELECT
  'Comments duplicated' as operation,
  COUNT(*) as count
FROM
  comment c
  JOIN issue_id_mapping mapping ON c."issueID" = mapping.new_issue_id
UNION
ALL
SELECT
  'Issue labels duplicated' as operation,
  COUNT(*) as count
FROM
  "issueLabel" il
  JOIN issue_id_mapping mapping ON il."issueID" = mapping.new_issue_id
UNION
ALL
SELECT
  'View states duplicated' as operation,
  COUNT(*) as count
FROM
  "viewState" vs
  JOIN issue_id_mapping mapping ON vs."issueID" = mapping.new_issue_id;

COMMIT;