-- The upstream database for the end-to-end tests: the tables of the zql test
-- schema (packages/zql/src/query/test/test-schemas.ts) under their server
-- names, with a small seed.

CREATE TABLE users (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  metadata JSONB
);

CREATE TABLE issues (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT NOT NULL,
  closed BOOLEAN NOT NULL,
  owner_id TEXT REFERENCES users(id),
  "createdAt" DOUBLE PRECISION NOT NULL
);

CREATE TABLE comments (
  id TEXT PRIMARY KEY,
  "authorId" TEXT NOT NULL,
  issue_id TEXT NOT NULL,
  text TEXT NOT NULL,
  "createdAt" DOUBLE PRECISION NOT NULL
);

CREATE TABLE label (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE "issueLabel" (
  "issueId" TEXT NOT NULL,
  "labelId" TEXT NOT NULL,
  PRIMARY KEY ("issueId", "labelId")
);

CREATE TABLE revision (
  id TEXT PRIMARY KEY,
  "authorId" TEXT NOT NULL,
  "commentId" TEXT NOT NULL,
  text TEXT NOT NULL
);

INSERT INTO users (id, name, metadata) VALUES
  ('u1', 'Alice', '{"registrar": "github", "email": "alice@example.com"}'),
  ('u2', 'Bob', NULL),
  ('u3', 'Çağrı', '{"registrar": "google", "email": "c@example.com", "altContacts": ["x"]}');

INSERT INTO issues (id, title, description, closed, owner_id, "createdAt") VALUES
  ('i1', 'Crash on launch', 'It crashes', false, 'u1', 1),
  ('i2', 'Slow sync', '', true, 'u2', 2),
  ('i3', 'Emoji 😀 in titles', 'é vs é', false, NULL, 3.5),
  ('i4', 'Dark mode', 'Please', false, 'u1', 4);

INSERT INTO comments (id, "authorId", issue_id, text, "createdAt") VALUES
  ('c1', 'u2', 'i1', 'Same here', 10),
  ('c2', 'u1', 'i1', 'Fixed?', 11),
  ('c3', 'u3', 'i3', '👍', 12);

INSERT INTO label (id, name) VALUES ('l1', 'bug'), ('l2', 'feature');

INSERT INTO "issueLabel" ("issueId", "labelId") VALUES
  ('i1', 'l1'), ('i2', 'l1'), ('i4', 'l2');

INSERT INTO revision (id, "authorId", "commentId", text) VALUES
  ('r1', 'u1', 'c2', 'Fixed!');
