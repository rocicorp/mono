

CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified = (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER issue_set_last_modified
BEFORE INSERT OR UPDATE ON issue
FOR EACH ROW
EXECUTE FUNCTION update_modified_column();

CREATE OR REPLACE FUNCTION issue_set_created_on_insert()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created = (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER issue_set_created_on_insert_trigger
BEFORE INSERT ON issue
FOR EACH ROW
EXECUTE FUNCTION issue_set_created_on_insert();


CREATE OR REPLACE FUNCTION update_issue_modified_time()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE issue
    SET modified = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000
    WHERE id = NEW."issueID";
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_issue_modified_time_on_comment
AFTER INSERT ON comment
FOR EACH ROW
EXECUTE FUNCTION update_issue_modified_time();

CREATE OR REPLACE FUNCTION comment_set_created_on_insert()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created = (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER comment_set_created_on_insert_trigger
BEFORE INSERT ON comment
FOR EACH ROW
EXECUTE FUNCTION comment_set_created_on_insert();

CREATE OR REPLACE FUNCTION validate_comment_body_length()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.body IS NOT NULL THEN
    -- The launch post has a special case maxlength of 1024 because trolls
    IF NEW."issueID" = 'duuW9Nyj5cTNLlimp9Qje' AND LENGTH(NEW.body) > 1024 THEN
      RAISE EXCEPTION 'Column value exceeds maximum allowed length of %', 1024;
    END IF;
    -- Length chosen because we have some old comments that are ~44KB.
    IF LENGTH(NEW.body) > 64*1024 THEN
      RAISE EXCEPTION 'Column value exceeds maximum allowed length of %', 64*1024;
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_comment_body_length
BEFORE INSERT OR UPDATE ON comment
FOR EACH ROW
EXECUTE FUNCTION validate_comment_body_length();


CREATE OR REPLACE FUNCTION emoji_check_subject_id()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if subjectID exists in the issue table
    IF EXISTS (SELECT 1 FROM issue WHERE id = NEW."subjectID") THEN
        NULL; -- Do nothing
    ELSIF EXISTS (SELECT 1 FROM comment WHERE id = NEW."subjectID") THEN
        NULL; -- Do nothing
    ELSE
        RAISE EXCEPTION 'id ''%'' does not exist in issue or comment', NEW."subjectID";
    END IF;
    
    PERFORM update_issue_modified_on_emoji_change(NEW."subjectID");

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER emoji_check_subject_id_update_trigger
BEFORE INSERT OR UPDATE ON emoji
FOR EACH ROW
EXECUTE FUNCTION emoji_check_subject_id();

CREATE OR REPLACE FUNCTION emoji_set_created_on_insert()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER emoji_set_created_on_insert_trigger
BEFORE INSERT ON emoji
FOR EACH ROW
EXECUTE FUNCTION emoji_set_created_on_insert();

-- Delete emoji when issue is deleted
CREATE OR REPLACE FUNCTION delete_emoji_on_issue_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM emoji WHERE "subjectID" = OLD.id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER delete_emoji_on_issue_delete_trigger
AFTER DELETE ON issue
FOR EACH ROW
EXECUTE FUNCTION delete_emoji_on_issue_delete();

-- Delete emoji when comment is deleted
CREATE OR REPLACE FUNCTION delete_emoji_on_comment_delete()
RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM emoji WHERE "subjectID" = OLD.id;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER delete_emoji_on_comment_delete_trigger
AFTER DELETE ON comment
FOR EACH ROW
EXECUTE FUNCTION delete_emoji_on_comment_delete();

-- When an emoji is added or deleted we find the issue and update the modified time
CREATE OR REPLACE FUNCTION update_issue_modified_on_emoji_change("subjectID" VARCHAR)
RETURNS VOID AS $$
BEGIN
    UPDATE issue
    SET modified = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000
    FROM (
        SELECT issue.id AS id
        FROM issue JOIN comment ON issue.id=comment."issueID"
        WHERE comment.id = "subjectID" OR issue.id = "subjectID"
    ) AS subquery
    WHERE issue.id = subquery.id;
END;   
$$ LANGUAGE plpgsql;

-- Create the indices on upstream so we can copy to downstream on replication.
-- We have discussed that, in the future, the indices of the Zero replica
-- can / should diverge from the indices of the upstream. This is because
-- the Zero replica could be serving a different set of applications than the
-- upstream. If that is true, it would be beneficial to have indices dedicated
-- to those use cases. This may not be true, however.
--
-- Until then, I think it makes the most sense to copy the indices from upstream
-- to the replica. The argument in favor of this is that it gives the user a single
-- place to manage indices and it saves us a step in setting up our demo apps.
CREATE INDEX issuelabel_issueid_idx ON "issueLabel" ("issueID");

CREATE INDEX issue_modified_idx ON issue (modified);

CREATE INDEX issue_created_idx ON issue (created);

CREATE INDEX issue_open_modified_idx ON issue (open, modified);

CREATE INDEX comment_issueid_idx ON "comment" ("issueID");

VACUUM;