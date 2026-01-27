# Synthetic Data Generation Guide

End-to-end process for generating ~1B rows of synthetic data and loading it into PostgreSQL for zbugs load testing.

## Prerequisites

1. **Gigabugs source data** — the generator reads source CSVs from `db/seed-data/gigabugs/` and multiplies them. Download them first:

   ```bash
   cd db/seed-data/gigabugs
   bash getData.sh
   ```

2. **Anthropic API key** — required for template generation (Step 1 only). Set `ANTHROPIC_API_KEY` in your environment.

3. **Docker** — required for running PostgreSQL locally.

## Step 1: Generate Templates

Templates are LLM-generated pools of issue titles, descriptions, comments, project names, labels, and components across 10 software categories. They use `{{slot}}` placeholders that get filled with randomized values during CSV generation.

```bash
ANTHROPIC_API_KEY=sk-... npm run generate-templates
```

Output: JSON files written to `db/seed-data/templates/` (one per category plus `summary.json`).

| Env Var | Default | Description |
|---------|---------|-------------|
| `ANTHROPIC_API_KEY` | *(required)* | Anthropic API key |
| `NUM_PROJECTS` | `100` | Total projects (divided across 10 categories) |

This step calls the Claude API in batches of 3 categories. Each category generates projects with components, labels, title/description/comment templates.

## Step 2: Generate CSV Data

Reads templates + gigabugs source CSVs and produces sharded CSV files with synthetic data.

```bash
npm run generate-synthetic
```

Output: sharded CSV files in `db/seed-data/synthetic/` named `{table}_{shard}.csv` (e.g., `issue_000.csv`, `issue_001.csv`, ...).

Tables generated: `user`, `project`, `label`, `issue`, `comment`, `issueLabel`.

| Env Var | Default | Description |
|---------|---------|-------------|
| `NUM_PROJECTS` | `100` | Total projects |
| `NUM_USERS` | `100` | Total users |
| `MULTIPLICATION_FACTOR` | `345` | Batch count — each source row is replicated this many times. Default yields ~83M issues. |
| `SHARD_SIZE` | `500000` | Rows per CSV shard file |
| `OUTPUT_DIR` | `db/seed-data/synthetic/` | Output directory |
| `SEED` | `42` | RNG seed for reproducible output |

### Scaling estimates

With the default `MULTIPLICATION_FACTOR=345` and the standard gigabugs source data:

- Issues: `source_issues * 345` (~83M with ~240K source issues)
- Comments: `source_comments * 345` (~comparable scale)
- IssueLabels: `source_labels * 345`

To reach ~1B total rows, increase `MULTIPLICATION_FACTOR` accordingly based on your source data size.

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

| Env Var | Default | Description |
|---------|---------|-------------|
| `ZERO_UPSTREAM_DB` | *(from `.env`)* | PostgreSQL connection string |
| `ZERO_SEED_BULK` | *(set by script)* | Enables bulk load optimizations |
| `ZERO_SEED_DATA_DIR` | *(set by script)* | Directory containing CSV shards |
| `ZERO_SEED_FORCE` | *(unset)* | Set to `true` to re-seed even if data exists |

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

-- Distribution across projects
SELECT "projectID", COUNT(*) AS issue_count
FROM "issue"
GROUP BY "projectID"
ORDER BY issue_count DESC
LIMIT 10;
```

## Alternative: Download Pre-generated Data

Instead of generating data locally, you can download pre-generated synthetic CSVs from S3:

```bash
cd db/seed-data/synthetic
bash getData.sh
```

This fetches a manifest from `s3://rocinante-dev/synthetic_v1/` and downloads all CSV shards. If the data hasn't been uploaded yet, the script will print instructions to generate locally instead.

After downloading, proceed to [Step 3](#step-3-start-postgresql) and [Step 4](#step-4-load-into-postgresql).
