-- Bug tracker schema (PostgreSQL)
-- Creates all objects in the public schema.

set search_path to public;

-- Enumerated types for issue fields
create type issue_status as enum (
  'open',
  'in_progress',
  'blocked',
  'resolved',
  'closed'
);

create type issue_priority as enum (
  'low',
  'medium',
  'high',
  'urgent'
);

-- Organizations / Teams / Projects
create table organization (
  id          bigserial primary key,
  name        text        not null,
  slug        text        not null,
  created_at  timestamptz not null default now(),
  unique (slug)
);

create table team (
  id              bigserial primary key,
  organization_id bigint      not null references organization(id) on delete cascade,
  name            text        not null,
  slug            text        not null,
  created_at      timestamptz not null default now(),
  unique (organization_id, slug)
);

create table project (
  id              bigserial primary key,
  organization_id bigint      not null references organization(id) on delete cascade,
  key             text        not null, -- short code like 'BUG'
  name            text        not null,
  slug            text        not null,
  description     text,
  created_at      timestamptz not null default now(),
  unique (organization_id, key),
  unique (organization_id, slug)
);

-- Accounts and memberships
create table account (
  id           bigserial primary key,
  email        text        not null,
  handle       text        not null,
  display_name text        not null,
  created_at   timestamptz not null default now(),
  unique (email),
  unique (handle)
);

create table org_member (
  organization_id bigint      not null references organization(id) on delete cascade,
  account_id      bigint      not null references account(id) on delete cascade,
  role            text        not null default 'member',
  joined_at       timestamptz not null default now(),
  primary key (organization_id, account_id)
);

create table project_member (
  project_id bigint      not null references project(id) on delete cascade,
  account_id bigint      not null references account(id) on delete cascade,
  role       text        not null default 'contributor',
  joined_at  timestamptz not null default now(),
  primary key (project_id, account_id)
);

-- Labels & milestones
create table label (
  id          bigserial primary key,
  project_id  bigint not null references project(id) on delete cascade,
  name        text   not null,
  color       text   not null default '#808080',
  description text,
  unique (project_id, name)
);

create table milestone (
  id          bigserial primary key,
  project_id  bigint      not null references project(id) on delete cascade,
  title       text        not null,
  description text,
  due_date    date,
  created_at  timestamptz not null default now()
);

-- Issues
create table issue (
  id           bigserial primary key,
  project_id   bigint             not null references project(id) on delete cascade,
  reporter_id  bigint                 references account(id) on delete set null,
  assignee_id  bigint                 references account(id) on delete set null,
  milestone_id bigint                 references milestone(id) on delete set null,
  title        text               not null,
  body         text,
  status       issue_status   not null default 'open',
  priority     issue_priority not null default 'medium',
  created_at   timestamptz        not null default now(),
  updated_at   timestamptz        not null default now()
);

create index issue_project_status_idx on issue (project_id, status);
create index issue_project_created_idx on issue (project_id, created_at desc);

-- Multi-assignments and watchers
create table issue_assignee (
  issue_id   bigint not null references issue(id) on delete cascade,
  account_id bigint not null references account(id) on delete cascade,
  primary key (issue_id, account_id)
);

create table issue_watcher (
  issue_id     bigint      not null references issue(id) on delete cascade,
  account_id   bigint      not null references account(id) on delete cascade,
  subscribed_at timestamptz not null default now(),
  primary key (issue_id, account_id)
);

-- Issue labels (many-to-many)
create table issue_label (
  issue_id bigint not null references issue(id) on delete cascade,
  label_id bigint not null references label(id) on delete cascade,
  primary key (issue_id, label_id)
);

create index issue_label_label_idx on issue_label (label_id);

-- Comments & reactions
create table comment (
  id         bigserial primary key,
  issue_id   bigint      not null references issue(id) on delete cascade,
  author_id  bigint          references account(id) on delete set null,
  body       text        not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index comment_issue_created_idx on comment (issue_id, created_at);

create table comment_reaction (
  comment_id bigint      not null references comment(id) on delete cascade,
  account_id bigint      not null references account(id) on delete cascade,
  emoji      text        not null,
  created_at timestamptz not null default now(),
  primary key (comment_id, account_id, emoji)
);

-- Attachments
create table attachment (
  id           bigserial primary key,
  issue_id     bigint      not null references issue(id) on delete cascade,
  uploader_id  bigint          references account(id) on delete set null,
  file_name    text        not null,
  content_type text,
  size_bytes   bigint      not null check (size_bytes >= 0),
  location     text        not null, -- URL or storage key
  created_at   timestamptz not null default now()
);

create index attachment_issue_idx on attachment (issue_id);

