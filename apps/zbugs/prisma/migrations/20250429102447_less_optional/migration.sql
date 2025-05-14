/*
  Warnings:

  - Made the column `issueID` on table `comment` required. This step will fail if there are existing NULL values in that column.
  - Made the column `created` on table `comment` required. This step will fail if there are existing NULL values in that column.
  - Made the column `creatorID` on table `comment` required. This step will fail if there are existing NULL values in that column.
  - Made the column `created` on table `emoji` required. This step will fail if there are existing NULL values in that column.
  - Made the column `modified` on table `issue` required. This step will fail if there are existing NULL values in that column.
  - Made the column `created` on table `issue` required. This step will fail if there are existing NULL values in that column.
  - Made the column `description` on table `issue` required. This step will fail if there are existing NULL values in that column.
  - Made the column `avatar` on table `user` required. This step will fail if there are existing NULL values in that column.

*/
-- AlterTable
ALTER TABLE "comment" ALTER COLUMN "issueID" SET NOT NULL,
ALTER COLUMN "created" SET NOT NULL,
ALTER COLUMN "creatorID" SET NOT NULL;

-- AlterTable
ALTER TABLE "emoji" ALTER COLUMN "created" SET NOT NULL;

-- AlterTable
ALTER TABLE "issue" ALTER COLUMN "modified" SET NOT NULL,
ALTER COLUMN "created" SET NOT NULL,
ALTER COLUMN "description" SET NOT NULL;

-- AlterTable
ALTER TABLE "user" ALTER COLUMN "avatar" SET NOT NULL;
