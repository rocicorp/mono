DO $$ 
DECLARE
    issue_id TEXT;
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
        'Thanks for raising this! It''s very insightful.',
        'This aligns well with what we discussed in the last meeting.',
        'I''ll take a closer look and share my feedback soon.',
        'It''s interesting how this affects the overall performance.',
        'This approach might work better for edge cases.'
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

    -- Delete all issues with title 'Test Issue'
    DELETE FROM issue WHERE title = 'Test Issue';

    -- Create new test issue and store its ID
    INSERT INTO issue (id, title, description, open, "creatorID")
    SELECT 
        gen_random_uuid()::TEXT,
        'Test Issue',
        'This is a Test Issue',
        true,
        id
    FROM "user"
    LIMIT 1
    RETURNING id INTO issue_id;

    -- Get all user IDs to randomly assign as comment creators
    SELECT ARRAY_AGG(id) INTO creator_ids
    FROM "user";

    -- Set base time to one year ago
    base_time := EXTRACT(EPOCH FROM NOW() - interval '1 year');

    -- Create 1000 comments for the new issue
    FOR i IN 1..1000 LOOP
        -- Generate a random UUID for the comment ID
        comment_id := gen_random_uuid()::TEXT;

        -- Generate a random comment body (1–10 sentences)
        comment_body := 'Comment #' || i || ': ' || array_to_string(
            ARRAY(
                SELECT
                    phrases[ceil(random() * array_length(phrases, 1))::INT]
                FROM
                    generate_series(1, ceil(random() * 10)::INT)
            ),
            ' '
        );

        -- Set creation time to increase by one hour for each comment
        creation_time := base_time + (i * 3600);

        -- Insert the comment
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
        -- Pick a random emoji and its annotation
        random_idx := 1 + floor(random() * array_length(emojis, 1))::INTEGER;
        -- Pick a random creator
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
            -- Pick a random emoji and its annotation
            random_idx := 1 + floor(random() * array_length(emojis, 1))::INTEGER;
            -- Pick a random creator
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