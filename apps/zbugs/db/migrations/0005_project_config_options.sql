ALTER TABLE "project" ADD COLUMN "issueCountEstimate" integer;--> statement-breakpoint
ALTER TABLE "project" ADD COLUMN "supportsSearch" boolean DEFAULT true;--> statement-breakpoint
ALTER TABLE "project" ADD COLUMN "markURL" varchar;--> statement-breakpoint
ALTER TABLE "project" ADD COLUMN "logoURL" varchar;