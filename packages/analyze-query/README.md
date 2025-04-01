# Analyze ZQL

Given a ZQL query, analyze its execution.

## Usage

Run this script from the same folder that contains the `.env` for your product as `analyze-zql` will need to connect
to your replica DB.

```bash
npx analyze-zql --query=your_query_string --schema=path_to_schema.ts
```

**Example:**

```bash
npx analyze-zql --query='issue.where("id", "=", 1).related("comments")' --schema=./shared/schema.ts
```
