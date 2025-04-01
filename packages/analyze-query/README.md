# Analyze ZQL

Two scripts are included here:

1. Transforms query hashes to their full blown AST and ZQL representation, with permissions applied
2. Analyzes the execution of a ZQL query.

## Usage

Run these scripts from the folder that contains the `.env` for your product as they need access to the schema, permissions and replica for your product. transform-query additionally needs cvr db access.

```bash
npx analyze-query --query=your_query_string --schema=path_to_schema.ts
npx transform-query  --hash=hash --schema=path_to_schema.ts
```

**Example:**

```bash
npx analyze-query --query='issue.where("id", "=", 1).related("comments")' --schema=./shared/schema.ts
npm run transform-query --hash=2i81bazy03a00 --path=./shared/schema.ts
```
