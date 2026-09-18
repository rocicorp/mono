----> BEGIN manual modification for triggers
-- Called from the BEFORE INSERT OR UPDATE trigger on emoji to bump the
-- modified time of the issue that was reacted to (directly, or via one of
-- its comments).
--
-- The previous version joined the entire issue table against the entire
-- comment table and filtered with an OR that referenced both sides, which
-- Postgres could only evaluate by sequentially scanning every comment. The
-- trigger runs inside the emoji insert's transaction, so on a large database
-- every reaction took seconds to commit and to replicate to other clients.
-- The inner join also meant an issue with no comments never got bumped.
--
-- Both branches below resolve to primary-key lookups.
CREATE OR REPLACE FUNCTION update_issue_modified_on_emoji_change("subjectID" VARCHAR)
RETURNS VOID AS $$
BEGIN
    UPDATE issue
    SET modified = EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000
    WHERE id = "subjectID"
       OR id = (SELECT "issueID" FROM comment WHERE comment.id = "subjectID");
END;
$$ LANGUAGE plpgsql;
----> END manual modification for triggers
