
COPY "user"
FROM
    '/data/users.csv' WITH CSV HEADER;

COPY "label"
FROM
    '/data/labels.csv' WITH CSV HEADER;

COPY "issue" ("id","shortID","title","open","modified","created","creatorID","assigneeID","description")
FROM
    '/data/issues.csv' WITH CSV HEADER;

COPY "issueLabel"
FROM
    '/data/issue_labels.csv' WITH CSV HEADER;

COPY "comment"
FROM
    '/data/comments.csv' WITH CSV HEADER;
