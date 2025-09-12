-- OpenSearch Sync Triggers for Real-time Updates
-- This file creates PostgreSQL triggers that notify when data changes occur

-- Create notification function for OpenSearch sync
CREATE OR REPLACE FUNCTION notify_opensearch_sync()
RETURNS TRIGGER AS $$
DECLARE
  payload JSON;
  issue_id VARCHAR;
BEGIN
  -- Determine the issue_id based on table and operation
  IF TG_TABLE_NAME = 'issue' THEN
    IF TG_OP = 'DELETE' THEN
      issue_id := OLD.id;
    ELSE
      issue_id := NEW.id;
    END IF;
  ELSIF TG_TABLE_NAME = 'comment' THEN
    IF TG_OP = 'DELETE' THEN
      issue_id := OLD."issueID";
    ELSE
      issue_id := NEW."issueID";
    END IF;
  ELSIF TG_TABLE_NAME = 'issueLabel' THEN
    IF TG_OP = 'DELETE' THEN
      issue_id := OLD."issueID";
    ELSE
      issue_id := NEW."issueID";
    END IF;
  ELSE
    -- For other tables, we might need different logic
    RETURN NEW;
  END IF;

  -- Build payload
  payload := json_build_object(
    'operation', TG_OP,
    'table', TG_TABLE_NAME,
    'issue_id', issue_id,
    'timestamp', extract(epoch from now()) * 1000
  );

  -- Send notification
  PERFORM pg_notify('opensearch_sync', payload::text);

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  ELSE
    RETURN NEW;
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Drop existing triggers if they exist
DROP TRIGGER IF EXISTS issue_opensearch_sync ON issue;
DROP TRIGGER IF EXISTS comment_opensearch_sync ON comment;
DROP TRIGGER IF EXISTS issueLabel_opensearch_sync ON "issueLabel";

-- Create triggers for issue table
CREATE TRIGGER issue_opensearch_sync
AFTER INSERT OR UPDATE OR DELETE ON issue
FOR EACH ROW
EXECUTE FUNCTION notify_opensearch_sync();

-- Create triggers for comment table
CREATE TRIGGER comment_opensearch_sync
AFTER INSERT OR UPDATE OR DELETE ON comment
FOR EACH ROW
EXECUTE FUNCTION notify_opensearch_sync();

-- Create triggers for issueLabel table (for label changes)
CREATE TRIGGER issueLabel_opensearch_sync
AFTER INSERT OR DELETE ON "issueLabel"
FOR EACH ROW
EXECUTE FUNCTION notify_opensearch_sync();

-- Also handle user updates (in case assignee/creator names change)
CREATE OR REPLACE FUNCTION notify_user_change()
RETURNS TRIGGER AS $$
DECLARE
  payload JSON;
BEGIN
  payload := json_build_object(
    'operation', 'USER_UPDATE',
    'table', 'user',
    'user_id', NEW.id,
    'timestamp', extract(epoch from now()) * 1000
  );
  
  PERFORM pg_notify('opensearch_sync', payload::text);
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS user_opensearch_sync ON "user";

CREATE TRIGGER user_opensearch_sync
AFTER UPDATE ON "user"
FOR EACH ROW
WHEN (OLD.login IS DISTINCT FROM NEW.login OR OLD.name IS DISTINCT FROM NEW.name)
EXECUTE FUNCTION notify_user_change();