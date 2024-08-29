DROP TABLE IF EXISTS "member", "issue", "comment", "label", "issueLabel" CASCADE;

CREATE TABLE member (
    "id" VARCHAR PRIMARY KEY,
    "name" VARCHAR NOT NULL
);
CREATE TABLE issue (
    "id" VARCHAR PRIMARY KEY,
    "title" VARCHAR NOT NULL,
    "priority" INTEGER NOT NULL CHECK (priority BETWEEN 1 AND 5),
    "status" INTEGER NOT NULL CHECK (status BETWEEN 1 AND 5),
    "modified" double precision NOT NULL,
    "created" double precision NOT NULL,
    "creatorID" VARCHAR REFERENCES member(id),
    "kanbanOrder" VARCHAR,
    "description" TEXT
);
CREATE TABLE comment (
    id VARCHAR PRIMARY KEY,
    "issueID" VARCHAR REFERENCES issue(id) ON DELETE CASCADE,
    "created" double precision,
    "body" TEXT NOT NULL,
    "creatorID" VARCHAR REFERENCES member(id)
);
CREATE TABLE label (
    "id" VARCHAR PRIMARY KEY,
    "name" VARCHAR NOT NULL
);
CREATE TABLE "issueLabel" (
    "id" VARCHAR PRIMARY KEY,
    "labelID" VARCHAR REFERENCES label(id),
    "issueID" VARCHAR REFERENCES issue(id) ON DELETE CASCADE
);

COPY "member" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/members.csv.gz' WITH CSV HEADER;
COPY "label" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/labels.csv.gz' WITH CSV HEADER;
COPY "issue" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issues.csv.gz' WITH CSV HEADER;
COPY "issue" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issues_1.csv.gz' WITH CSV HEADER;
COPY "issue" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issues_2.csv.gz' WITH CSV HEADER;
COPY "issue" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issues_3.csv.gz' WITH CSV HEADER;
COPY "issue" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issues_4.csv.gz' WITH CSV HEADER;
COPY "issue" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issues_5.csv.gz' WITH CSV HEADER;
COPY "issue" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issues_6.csv.gz' WITH CSV HEADER;
COPY "issueLabel" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issue_labels.csv.gz' WITH CSV HEADER;
COPY "issueLabel" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issue_labels_1.csv.gz' WITH CSV HEADER;
COPY "issueLabel" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issue_labels_2.csv.gz' WITH CSV HEADER;
COPY "issueLabel" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issue_labels_3.csv.gz' WITH CSV HEADER;
COPY "issueLabel" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issue_labels_4.csv.gz' WITH CSV HEADER;
COPY "issueLabel" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issue_labels_5.csv.gz' WITH CSV HEADER;
COPY "issueLabel" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/issue_labels_6.csv.gz' WITH CSV HEADER;
COPY "comment" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/comments.csv.gz' WITH CSV HEADER;
COPY "comment" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/comments_1.csv.gz' WITH CSV HEADER;
COPY "comment" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/comments_2.csv.gz' WITH CSV HEADER;
COPY "comment" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/comments_3.csv.gz' WITH CSV HEADER;
COPY "comment" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/comments_4.csv.gz' WITH CSV HEADER;
COPY "comment" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/comments_5.csv.gz' WITH CSV HEADER;
COPY "comment" FROM PROGRAM 'gzip -cd /docker-entrypoint-initdb.d/comments_6.csv.gz' WITH CSV HEADER;

-- Create the indices on upstream so we can copy to downstream on replication.
-- We have discussed that, in the future, the indices of the Zero replica
-- can / should diverge from the indices of the upstream. This is because
-- the Zero replica could be serving a different set of applications than the
-- upstream. If that is true, it would be beneficial to have indices dedicated
-- to those use cases. This may not be true, however.
--
-- Until then, I think it makes the most sense to copy the indices from upstream
-- to the replica. The argument in favor of this is that it gives the user a single
-- place to manage indices and it saves us a step in setting up our demo apps.
CREATE INDEX issuelabel_issueid_idx ON "issueLabel" ("issueID");
CREATE INDEX issue_modified_idx ON issue (modified);
CREATE INDEX issue_created_idx ON issue (created);
CREATE INDEX issue_priority_modified_idx ON issue (priority,modified);
CREATE INDEX issue_status_modified_idx ON issue (status,modified);
CREATE INDEX comment_issueid_idx ON "comment" ("issueID");

SELECT * FROM pg_create_logical_replication_slot('zero_slot_r1', 'pgoutput');

VACUUM;
