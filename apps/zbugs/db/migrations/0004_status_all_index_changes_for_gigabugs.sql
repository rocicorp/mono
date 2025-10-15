CREATE INDEX "issue_projectID_assigneeID_modified_idx" ON "issue" USING btree ("projectID","assigneeID","modified","id");--> statement-breakpoint
CREATE INDEX "issue_projectID_creatorID_modified_idx" ON "issue" USING btree ("projectID","creatorID","modified","id");--> statement-breakpoint
CREATE INDEX "issue_projectID_modified_idx" ON "issue" USING btree ("projectID","modified","id");