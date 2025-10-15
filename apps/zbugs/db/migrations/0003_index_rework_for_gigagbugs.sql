DROP INDEX "issue_modified_idx";--> statement-breakpoint
DROP INDEX "issue_open_modified_idx";--> statement-breakpoint
ALTER TABLE "project" ALTER COLUMN "lowerCaseName" SET DEFAULT '';--> statement-breakpoint
CREATE INDEX "issue_shortID_idx" ON "issue" USING btree ("shortID");--> statement-breakpoint
CREATE INDEX "issue_projectID_open_assigneeID_modified_idx" ON "issue" USING btree ("projectID","open","assigneeID","modified","id");--> statement-breakpoint
CREATE INDEX "issue_projectID_open_creatorID_modified_idx" ON "issue" USING btree ("projectID","open","creatorID","modified","id");--> statement-breakpoint
CREATE INDEX "issue_projectID_open_modified_idx" ON "issue" USING btree ("projectID","open","modified","id");--> statement-breakpoint
CREATE INDEX "issue_creatorID_idx" ON "issue" USING btree ("creatorID","id");--> statement-breakpoint
CREATE INDEX "issue_assigneeID_idx" ON "issue" USING btree ("assigneeID","id");