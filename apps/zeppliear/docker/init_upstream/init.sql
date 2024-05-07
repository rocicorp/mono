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
    "issueID" VARCHAR REFERENCES issue(id),
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
    "issueID" VARCHAR REFERENCES issue(id)
);
COPY "member" FROM '/docker-entrypoint-initdb.d/members.csv' WITH CSV HEADER;
COPY "label" FROM '/docker-entrypoint-initdb.d/labels.csv' WITH CSV HEADER;
COPY "issue" FROM '/docker-entrypoint-initdb.d/issues.csv' WITH CSV HEADER;
COPY "comment" FROM '/docker-entrypoint-initdb.d/comments.csv' WITH CSV HEADER;
COPY "issueLabel" FROM '/docker-entrypoint-initdb.d/issue_labels.csv' WITH CSV HEADER;
