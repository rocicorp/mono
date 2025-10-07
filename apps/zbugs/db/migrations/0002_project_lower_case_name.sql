

DROP INDEX "project_name_idx";--> statement-breakpoint
ALTER TABLE "project" ADD COLUMN "lowerCaseName" varchar;--> statement-breakpoint
CREATE UNIQUE INDEX "project_lower_case_name_idx" ON "project" USING btree ("lowerCaseName");

----> BEGIN manual modification for triggers
CREATE OR REPLACE FUNCTION project_set_lower_case_name()
RETURNS TRIGGER AS $$
BEGIN
    NEW."lowerCaseName" := LOWER(NEW.name);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER project_set_lowercase_name_on_insert_or_update_trigger
BEFORE INSERT OR UPDATE ON project
FOR EACH ROW
EXECUTE FUNCTION project_set_lower_case_name();
----> END manual modification for triggers

----> BEGIN manual modification for backfill
-- Lock the table to prevent ALL concurrent writes (INSERT, UPDATE, DELETE).
-- This avoids rows being inserted before trigger is committed but after
-- the backfill.
-- Concurrent SELECTs are still allowed.
LOCK TABLE project IN SHARE ROW EXCLUSIVE MODE;

UPDATE "project"
SET "lowerCaseName" = LOWER("name")
WHERE "lowerCaseName" IS NULL;

ALTER TABLE "project"
ALTER COLUMN "lowerCaseName" SET NOT NULL;
----> END manual modification for backfill
