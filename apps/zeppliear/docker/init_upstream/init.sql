DROP TABLE IF EXISTS "member", "issue", "comment", "label", "issueLabel" CASCADE;
VACUUM;
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
