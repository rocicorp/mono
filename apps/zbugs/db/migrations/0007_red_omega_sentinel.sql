DROP INDEX "label_name_idx";--> statement-breakpoint
CREATE UNIQUE INDEX "label_name_project_idx" ON "label" USING btree ("projectID","name");