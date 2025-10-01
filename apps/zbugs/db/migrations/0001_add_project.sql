CREATE TABLE "project" (
	"id" varchar PRIMARY KEY NOT NULL,
	"name" varchar NOT NULL
);
--> BEGIN manual modification for inserting Zero project
INSERT INTO "project" ("id", "name") VALUES ('iCNlS2qEpzYWEes1RTf-D', 'Zero');
--> END manual modification for inserting Zero project
--> statement-breakpoint
ALTER TABLE "issueLabel" DROP CONSTRAINT "issueLabel_labelID_fkey";
--> statement-breakpoint
ALTER TABLE "issueLabel" DROP CONSTRAINT "issueLabel_issueID_fkey";
--> statement-breakpoint
ALTER TABLE "issue" ADD COLUMN "projectID" varchar DEFAULT 'iCNlS2qEpzYWEes1RTf-D' NOT NULL;--> statement-breakpoint
ALTER TABLE "issueLabel" ADD COLUMN "projectID" varchar DEFAULT 'iCNlS2qEpzYWEes1RTf-D' NOT NULL;--> statement-breakpoint
ALTER TABLE "label" ADD COLUMN "projectID" varchar DEFAULT 'iCNlS2qEpzYWEes1RTf-D' NOT NULL;--> statement-breakpoint
CREATE UNIQUE INDEX "project_name_idx" ON "project" USING btree ("name");--> statement-breakpoint
CREATE UNIQUE INDEX "issue_project_idx" ON "issue" USING btree ("id","projectID");--> statement-breakpoint
CREATE UNIQUE INDEX "label_project_idx" ON "label" USING btree ("id","projectID");
ALTER TABLE "issue" ADD CONSTRAINT "issue_projectID_fkey" FOREIGN KEY ("projectID") REFERENCES "public"."project"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "issueLabel" ADD CONSTRAINT "issueLabel_labelID_projectID_fkey" FOREIGN KEY ("labelID","projectID") REFERENCES "public"."label"("id","projectID") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "issueLabel" ADD CONSTRAINT "issueLabel_issueID_projectID_fkey" FOREIGN KEY ("issueID","projectID") REFERENCES "public"."issue"("id","projectID") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "label" ADD CONSTRAINT "label_projectID_fkey" FOREIGN KEY ("projectID") REFERENCES "public"."project"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint

----> BEGIN manual modification for publication
ALTER PUBLICATION zero_zbugs ADD TABLE project;
----> END manual modification for publication