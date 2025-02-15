COPY "user"
FROM
    '/data/users.csv' WITH CSV HEADER;

COPY "label"
FROM
    '/data/labels.csv' WITH CSV HEADER;

COPY "issue"
FROM
    '/data/issues.csv' WITH CSV HEADER;

COPY "issue"
FROM
    '/data/issues_2.csv' WITH CSV HEADER;

COPY "issue"
FROM
    '/data/issues_3.csv' WITH CSV HEADER;

COPY "issue"
FROM
    '/data/issues_4.csv' WITH CSV HEADER;

COPY "issue"
FROM
    '/data/issues_5.csv' WITH CSV HEADER;

COPY "issue"
FROM
    '/data/issues_6.csv' WITH CSV HEADER;

COPY "issueLabel"
FROM
    '/data/issue_labels.csv' WITH CSV HEADER;

COPY "issueLabel"
FROM
    '/data/issue_labels_2.csv' WITH CSV HEADER;

COPY "issueLabel"
FROM
    '/data/issue_labels_3.csv' WITH CSV HEADER;

COPY "issueLabel"
FROM
    '/data/issue_labels_4.csv' WITH CSV HEADER;

COPY "issueLabel"
FROM
    '/data/issue_labels_5.csv' WITH CSV HEADER;

COPY "issueLabel"
FROM
    '/data/issue_labels_6.csv' WITH CSV HEADER;

COPY "comment"
FROM
    '/data/comments.csv' WITH CSV HEADER;

COPY "comment"
FROM
    '/data/comments_2.csv' WITH CSV HEADER;

COPY "comment"
FROM
    '/data/comments_3.csv' WITH CSV HEADER;

COPY "comment"
FROM
    '/data/comments_4.csv' WITH CSV HEADER;

COPY "comment"
FROM
    '/data/comments_5.csv' WITH CSV HEADER;

COPY "comment"
FROM
    '/data/comments_6.csv' WITH CSV HEADER;