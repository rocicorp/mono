DO $$ 
DECLARE
    issue_id TEXT := 'f8218601-c64a-43d5-8bf6-aa7c11f5d571';
    comment_id TEXT;
    comment_body TEXT;
    creation_time DOUBLE PRECISION;
    random_idx INTEGER;
    random_creator TEXT;
    emojis TEXT[];
    emoji_annotations TEXT[];
    creator_ids TEXT[];
    comment_record RECORD;
    base_time DOUBLE PRECISION;
    phrases TEXT[] := ARRAY[
        'This is a really interesting issue.',
        'I think this could be improved by adding more details.',
        'Great point! I completely agree.',
        'Have you considered alternative approaches?',
        'This reminds me of a similar problem I worked on recently.',
        'Looking forward to hearing more updates about this.',
        'I think there might be a typo in the documentation.',
        'Could we add an example to clarify this?',
        'This solution is elegant and well thought out.',
        'I have a few questions about the implementation details.',
        'Thanks for raising this! It’s very insightful.',
        'This aligns well with what we discussed in the last meeting.',
        'I’ll take a closer look and share my feedback soon.',
        'It’s interesting how this affects the overall performance.',
        'This approach might work better for edge cases.',
        'Here’s what I found in my testing so far.',
        'Do we have benchmarks to support this claim?',
        'This would make the code much cleaner and more maintainable.',
        'Let me know if you need any help with this!',
        'I added a few comments inline for clarification.',
        'This seems like a corner case we might have missed.',
        'I wonder if this could be optimized further.',
        'Have you tested this with larger datasets?',
        'Does this align with the original design goals?',
        'We might need to update the documentation for this.',
        'Is this backward-compatible with older versions?',
        'Could you provide some context on why this is needed?',
        'It’s great to see progress on this!',
        'This could potentially break something downstream.',
        'What’s the timeline for getting this live?',
        'The code looks good, but I’d suggest running more tests.',
        'This is a good step forward, but there’s more to discuss.',
        'What happens if this fails under load?',
        'I’d suggest adding more comments to the code for clarity.',
        'Does this handle all edge cases, or are there gaps?',
        'Thanks for your hard work on this!',
        'I really appreciate the level of detail here.',
        'Could this be simplified to make it easier to maintain?',
        'Let’s make sure we communicate these changes clearly to the team.',
        'I like the approach, but it seems a bit over-engineered.',
        'This seems to align well with our objectives.',
        'Are there any security concerns with this implementation?',
        'This change looks great—approved!',
        'Can we add unit tests to cover this behavior?',
        'It might be worth running this through a quick peer review.',
        'Have you considered using a different library for this?',
        'This should help improve performance significantly.',
        'Are we confident this won’t introduce regressions?',
        'It would be helpful to see an example input/output.'
    ];
BEGIN
    -- Initialize emoji arrays
    emojis := ARRAY[
        '👍', '👎', '😄', '🎉', '😕', '❤️', '🚀', '👀', '🤔', '💯',
        '✨', '🔥', '💪', '👏', '🙌', '🎨', '💡', '⭐', '💫', '🌟'
    ];
    emoji_annotations := ARRAY[
        'thumbs up', 'thumbs down', 'smile', 'party', 'confused', 'heart', 'rocket', 'eyes', 'thinking', '100',
        'sparkles', 'fire', 'muscle', 'clap', 'raised hands', 'art', 'bulb', 'star', 'dizzy', 'glowing star'
    ];

    -- Delete existing comments and emojis
    DELETE FROM comment WHERE "issueID" = issue_id;
        DELETE FROM emoji 
    WHERE "subjectID" = issue_id
    OR "subjectID" IN (
        SELECT id FROM comment 
        WHERE "issueID" = issue_id
    );

    -- Get all user IDs to randomly assign as comment creators
    SELECT ARRAY_AGG(id) INTO creator_ids
    FROM "user";

    -- Set base time to one year ago
    base_time := EXTRACT(EPOCH FROM NOW() - interval '1 year');

    -- Create 1000 comments for the issue
    FOR i IN 1..1000 LOOP
        comment_id := gen_random_uuid()::TEXT;
        
        comment_body := 'Comment #' || i || ': ' || array_to_string(
            ARRAY(
                SELECT phrases[ceil(random() * array_length(phrases, 1))::INT]
                FROM generate_series(1, ceil(random() * 10)::INT)
            ),
            ' '
        );

        creation_time := base_time + (i * 3600);

        INSERT INTO comment (id, "issueID", created, body, "creatorID")
        VALUES (
            comment_id, 
            issue_id, 
            creation_time, 
            comment_body, 
            creator_ids[ceil(random() * array_length(creator_ids, 1))::INT]
        );
    END LOOP;

    -- Add 100 random emojis to the issue itself
    FOR i IN 1..100 LOOP
        random_idx := 1 + floor(random() * array_length(emojis, 1))::INTEGER;
        random_creator := creator_ids[1 + floor(random() * array_length(creator_ids, 1))::INTEGER];
        
        INSERT INTO emoji (id, value, annotation, "subjectID", "creatorID")
        SELECT 
            gen_random_uuid()::TEXT,
            emojis[random_idx],
            emoji_annotations[random_idx],
            issue_id,
            random_creator
        WHERE NOT EXISTS (
            SELECT 1 FROM emoji e 
            WHERE e."subjectID" = issue_id 
            AND e."creatorID" = random_creator
            AND e.value = emojis[random_idx]
        );
    END LOOP;

    -- Add 5 random emojis to each comment
    FOR comment_record IN 
        SELECT id FROM comment WHERE "issueID" = issue_id
    LOOP
        FOR i IN 1..5 LOOP
            random_idx := 1 + floor(random() * array_length(emojis, 1))::INTEGER;
            random_creator := creator_ids[1 + floor(random() * array_length(creator_ids, 1))::INTEGER];
            
            INSERT INTO emoji (id, value, annotation, "subjectID", "creatorID")
            SELECT 
                gen_random_uuid()::TEXT,
                emojis[random_idx],
                emoji_annotations[random_idx],
                comment_record.id,
                random_creator
            WHERE NOT EXISTS (
                SELECT 1 FROM emoji e 
                WHERE e."subjectID" = comment_record.id 
                AND e."creatorID" = random_creator
                AND e.value = emojis[random_idx]
            );
        END LOOP;
    END LOOP;
END $$;