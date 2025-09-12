-- Setup for PGSync logical replication
-- This creates the necessary publication for PGSync to track changes

-- Create publication for all tables that PGSync needs to track
DROP PUBLICATION IF EXISTS pgsync;

CREATE PUBLICATION pgsync FOR TABLE 
    issue,
    comment,
    "user",
    label,
    "issueLabel";

-- Note: PGSync will automatically create its own replication slot
-- named 'pgsync' when it starts up