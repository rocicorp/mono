#!/usr/bin/env bash
# Apply the prod-shaped fixture to a dedicated Postgres database.
#
# The SQL is idempotent: it deletes every `*_emu_lag_*` row first, so re-running
# it restores the fixture exactly. It never touches rows outside that namespace.
#
#   ./apply-seed.sh 'postgresql://postgres:postgres@localhost:5432/zcbaseline?sslmode=disable'
set -euo pipefail

DATABASE_URL="${1:?usage: apply-seed.sh <postgres-url> [client-count]}"
CLIENT_COUNT="${2:-32}"
SEED_SQL="$(dirname "$0")/seed-emulation.sql"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -f "$SEED_SQL"

# One better-auth session row per concurrent client. The replay driver sends
# `emu-session-token-NNN` as its Zero auth token, zero-cache forwards it to the
# goblins transform server as a bearer token, and better-auth resolves it to the
# emulation teacher. Concurrent groups need distinct tokens so nothing dedupes.
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -c "
INSERT INTO session (id, expires_at, token, updated_at, user_id)
SELECT 'session_emu_lag_' || lpad(n::text, 3, '0'),
       '2036-01-01T00:00:00Z',
       'emu-session-token-' || lpad(n::text, 3, '0'),
       now(),
       'user_emu_lag_teacher'
FROM generate_series(1, ${CLIENT_COUNT}) n
ON CONFLICT (id) DO NOTHING;"

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "
SELECT (SELECT count(*) FROM problem_tracker WHERE assignment_id = 'assignment_emu_lag_136') AS trackers,
       (SELECT count(*) FROM conversation WHERE id LIKE 'conversation_emu_lag_%') AS conversations,
       (SELECT count(*) FROM mastery_assessment WHERE id LIKE 'mastery_emu_lag_%') AS mastery,
       (SELECT count(*) FROM student WHERE id LIKE 'student_emu_lag_%') AS students,
       (SELECT count(*) FROM class WHERE id LIKE 'class_emu_lag_%') AS classes,
       (SELECT count(*) FROM session WHERE user_id = 'user_emu_lag_teacher') AS sessions;"
