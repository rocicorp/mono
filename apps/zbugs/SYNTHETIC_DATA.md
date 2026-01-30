# Synthetic Data Generation Guide

End-to-end process for generating large-scale synthetic data and loading it into PostgreSQL for zbugs load testing.

## Prerequisites

1. **Templates** — LLM-generated templates must exist in `db/seed-data/templates/`. If not present, run Step 1 first.

2. **Anthropic API key** — required for template generation (Step 1 only). Set `ANTHROPIC_API_KEY` in your environment.

3. **Docker** — required for running PostgreSQL locally.

## Step 1: Generate Templates

Templates are LLM-generated pools of issue titles, descriptions, comments, project names, labels, and components across 10 software categories. They use `{{slot}}` placeholders that get filled with randomized values during CSV generation.

```bash
ANTHROPIC_API_KEY=sk-... npm run generate-templates
```

Output: JSON files written to `db/seed-data/templates/` (one per category plus `summary.json`).

| Env Var             | Default      | Description                                   |
| ------------------- | ------------ | --------------------------------------------- |
| `ANTHROPIC_API_KEY` | _(required)_ | Anthropic API key                             |
| `NUM_PROJECTS`      | `100`        | Total projects (divided across 10 categories) |

This step calls the Claude API in batches of 3 categories. Each category generates projects with components, labels, title/description/comment templates.

## Step 2: Generate CSV Data

Self-contained generator that produces sharded CSV files based on configuration parameters and templates.

```bash
npm run generate-synthetic
```

Output: sharded CSV files in `db/seed-data/synthetic/` named `{table}_{shard}.csv` (e.g., `issue_000.csv`, `issue_001.csv`, ...).

Tables generated: `user`, `project`, `label`, `issue`, `comment`, `issueLabel`.

| Env Var              | Default                   | Description                               |
| -------------------- | ------------------------- | ----------------------------------------- |
| `NUM_ISSUES`         | `1000000`                 | Total issues to generate                  |
| `NUM_PROJECTS`       | `100`                     | Total projects                            |
| `NUM_USERS`          | `100`                     | Total users                               |
| `COMMENTS_PER_ISSUE` | `3.0`                     | Average comments per issue                |
| `LABELS_PER_ISSUE`   | `1.5`                     | Average labels per issue                  |
| `SHARD_SIZE`         | `500000`                  | Rows per CSV shard file                   |
| `OUTPUT_DIR`         | `db/seed-data/synthetic/` | Output directory                          |
| `SEED`               | `42`                      | RNG seed for reproducible output          |
| `SKIP_USERS`         | `false`                   | Skip user CSV generation (for batch mode) |

### Distribution Strategy

The generator uses realistic distributions for data:

- **Issues across projects**: Pareto/power-law distribution — some projects get many more issues than others
- **Comments across issues**: Pareto distribution — most issues have few comments, some have many
- **Labels per issue**: Uniform distribution — 1-3 labels per issue

### Scaling Examples

```bash
# Small test dataset (~10K issues)
NUM_ISSUES=10000 npm run generate-synthetic

# Medium dataset (~1M issues, default)
npm run generate-synthetic

# Large dataset (~100M issues)
NUM_ISSUES=100000000 COMMENTS_PER_ISSUE=2.0 npm run generate-synthetic
```

With default settings (`NUM_ISSUES=1000000`, `COMMENTS_PER_ISSUE=3.0`, `LABELS_PER_ISSUE=1.5`):

- Issues: 1,000,000
- Comments: ~3,000,000
- IssueLabels: ~1,500,000
- Total rows: ~4.5M

## Step 3: Start PostgreSQL

```bash
npm run db-up
```

Once PostgreSQL is running, apply the schema migrations:

```bash
npm run db-migrate
```

## Step 4: Load into PostgreSQL

The `db-seed-synthetic` script loads synthetic CSVs using PostgreSQL `COPY` with bulk optimizations:

```bash
npm run db-seed-synthetic
```

This runs `seed.ts` with `ZERO_SEED_BULK=true` and `ZERO_SEED_DATA_DIR=db/seed-data/synthetic`. Bulk mode:

1. Sets `maintenance_work_mem = 2GB` and `work_mem = 256MB`
2. Disables all triggers
3. Drops all foreign key constraints and non-PK indexes
4. Loads data via `COPY ... FROM STDIN CSV HEADER` (one file at a time, no wrapping transaction)
5. Recreates indexes
6. Recreates foreign key constraints
7. Re-enables triggers
8. Runs `ANALYZE` on all tables

To force re-seeding an already-seeded database:

```bash
ZERO_SEED_FORCE=true npm run db-seed-synthetic
```

| Env Var              | Default           | Description                                     |
| -------------------- | ----------------- | ----------------------------------------------- |
| `ZERO_UPSTREAM_DB`   | _(from `.env`)_   | PostgreSQL connection string                    |
| `ZERO_SEED_BULK`     | _(set by script)_ | Enables bulk load optimizations                 |
| `ZERO_SEED_DATA_DIR` | _(set by script)_ | Directory containing CSV shards                 |
| `ZERO_SEED_FORCE`    | _(unset)_         | Set to `true` to re-seed even if data exists    |
| `ZERO_SEED_APPEND`   | _(unset)_         | Set to `true` to append data without truncating |

## Generating Additional Batches

You can generate multiple batches of data that coexist in the same database without ID collisions. Non-user entity IDs use seeded nanoid, so different seeds produce globally unique IDs.

### Batch Workflow

```bash
# Batch 1: generate and load the initial dataset
NUM_ISSUES=100000000 NUM_PROJECTS=100 SEED=42 npm run generate-synthetic
npm run db-seed-synthetic

# Batch 2: generate additional data with a different seed
NUM_ISSUES=100000000 NUM_PROJECTS=1000 \
  SKIP_USERS=true \
  SEED=99 \
  OUTPUT_DIR=db/seed-data/synthetic-batch-2/ \
  npm run generate-synthetic

# Load batch 2 in append mode
ZERO_SEED_APPEND=true \
  ZERO_SEED_DATA_DIR=db/seed-data/synthetic-batch-2/ \
  npm run db-seed-synthetic
```

### Key points

- **Different `SEED` per batch** — each seed produces a different RNG sequence, yielding unique nanoid IDs and different content.
- **`SKIP_USERS=true`** — skips writing the user CSV since users are shared across batches. User IDs are sequential (`usr_0000`, `usr_0001`, ...) and are always the same regardless of `SEED`.
- **`ZERO_SEED_APPEND=true`** — tells the seeder to skip the "already seeded" check and skip truncation, appending data to existing tables. Bulk optimizations (drop indexes, COPY, recreate indexes, ANALYZE) still run.
- **`OUTPUT_DIR`** — point each batch at a separate directory so CSVs don't overwrite each other.

## Verification

After loading, verify row counts and data integrity:

```sql
-- Row counts
SELECT 'user' AS table_name, COUNT(*) FROM "user"
UNION ALL SELECT 'project', COUNT(*) FROM "project"
UNION ALL SELECT 'label', COUNT(*) FROM "label"
UNION ALL SELECT 'issue', COUNT(*) FROM "issue"
UNION ALL SELECT 'comment', COUNT(*) FROM "comment"
UNION ALL SELECT 'issueLabel', COUNT(*) FROM "issueLabel";

-- Check referential integrity
SELECT COUNT(*) AS orphaned_issues
FROM "issue" i
LEFT JOIN "project" p ON i."projectID" = p."id"
WHERE p."id" IS NULL;

SELECT COUNT(*) AS orphaned_comments
FROM "comment" c
LEFT JOIN "issue" i ON c."issueID" = i."id"
WHERE i."id" IS NULL;

-- Distribution across projects (should show Pareto-like distribution)
SELECT "projectID", COUNT(*) AS issue_count
FROM "issue"
GROUP BY "projectID"
ORDER BY issue_count DESC
LIMIT 10;

-- Comments per issue distribution
SELECT
  CASE
    WHEN comment_count = 0 THEN '0'
    WHEN comment_count BETWEEN 1 AND 2 THEN '1-2'
    WHEN comment_count BETWEEN 3 AND 5 THEN '3-5'
    WHEN comment_count BETWEEN 6 AND 10 THEN '6-10'
    ELSE '10+'
  END AS bucket,
  COUNT(*) AS issue_count
FROM (
  SELECT i.id, COUNT(c.id) AS comment_count
  FROM "issue" i
  LEFT JOIN "comment" c ON c."issueID" = i.id
  GROUP BY i.id
) sub
GROUP BY bucket
ORDER BY bucket;
```

## Alternative: Download Pre-generated Data

Instead of generating data locally, you can download pre-generated synthetic CSVs from S3:

```bash
cd db/seed-data/synthetic
bash getData.sh
```

This fetches a manifest from `s3://rocinante-dev/synthetic_v1/` and downloads all CSV shards. If the data hasn't been uploaded yet, the script will print instructions to generate locally instead.

After downloading, proceed to [Step 3](#step-3-start-postgresql) and [Step 4](#step-4-load-into-postgresql).
