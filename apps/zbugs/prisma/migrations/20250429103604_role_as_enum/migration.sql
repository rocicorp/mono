-- CreateEnum
CREATE TYPE "Role" AS ENUM ('user', 'crew');

-- Add the new enum column
ALTER TABLE "user" ADD COLUMN "role_new" "Role";

-- Migrate old string values to the new enum column
UPDATE "user" SET "role_new" = "role"::"Role";

-- Drop the old column
ALTER TABLE "user" DROP COLUMN "role";

-- Rename the new column
ALTER TABLE "user" RENAME COLUMN "role_new" TO "role";
