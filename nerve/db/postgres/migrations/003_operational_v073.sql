-- PostgreSQL version 3: operational model through SQLite v073.
-- Run as the schema administrator with BYPASSRLS, with daemons stopped.
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended('nerve_pg.schema', 0));
SET LOCAL search_path = nerve_pg, public;
DO $upgrade$
DECLARE
    installed integer;
    entry record;
    source_id text;
    turn_id text;
    first_event bigint;
BEGIN
    SELECT version INTO STRICT installed FROM storage_version;
    IF installed = 3 THEN RETURN; END IF;
    IF installed != 2 THEN
        RAISE EXCEPTION 'Expected PostgreSQL schema 2, found %', installed;
    END IF;
    IF NOT (SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname=current_user) THEN
        RAISE EXCEPTION 'Schema upgrade requires an administrator with BYPASSRLS';
    END IF;

    CREATE FUNCTION sqlite_date(value text) RETURNS text LANGUAGE sql STABLE AS $fn$
        SELECT to_char(value::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD')
    $fn$;
    CREATE FUNCTION sqlite_date(value text, modifier text) RETURNS text LANGUAGE sql STABLE AS $fn$
        SELECT to_char((value::timestamptz + modifier::interval) AT TIME ZONE 'UTC', 'YYYY-MM-DD')
    $fn$;

    CREATE FUNCTION sqlite_date(value timestamptz) RETURNS text LANGUAGE sql STABLE AS $fn$
        SELECT to_char(value AT TIME ZONE 'UTC', 'YYYY-MM-DD')
    $fn$;
    CREATE FUNCTION sqlite_date(value timestamp) RETURNS text LANGUAGE sql IMMUTABLE AS $fn$
        SELECT to_char(value, 'YYYY-MM-DD')
    $fn$;

    CREATE FUNCTION safe_json(value text) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE STRICT AS $fn$
    BEGIN
        RETURN value::jsonb;
    EXCEPTION WHEN invalid_text_representation THEN
        RETURN NULL;
    END
    $fn$;
    CREATE FUNCTION json_valid(value text) RETURNS boolean LANGUAGE sql IMMUTABLE AS $fn$
        SELECT nerve_pg.safe_json(value) IS NOT NULL
    $fn$;
    -- Operational stores extract top-level string fields, not arbitrary JSON paths.
    CREATE FUNCTION json_extract(value text, path text) RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $fn$
    BEGIN
        IF path !~ '^\$\.[A-Za-z_][A-Za-z_0-9]*$' THEN
            RAISE EXCEPTION 'Unsupported operational JSON path: %', path;
        END IF;
        RETURN nerve_pg.safe_json(value) ->> substr(path, 3);
    END
    $fn$;

    CREATE FUNCTION glob_matches(value text, pattern text) RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT AS $fn$
    DECLARE
        expression text := '\A';
        pos integer := 1;
        ending integer;
        ch text;
        members text;
    BEGIN
        WHILE pos <= length(pattern) LOOP
            ch := substr(pattern, pos, 1);
            IF ch = '*' THEN expression := expression || '.*';
            ELSIF ch = '?' THEN expression := expression || '.';
            ELSIF ch = '[' THEN
                ending := pos + 1;
                IF substr(pattern, ending, 1) = '^' THEN ending := ending + 1; END IF;
                IF substr(pattern, ending, 1) = ']' THEN ending := ending + 1; END IF;
                WHILE ending <= length(pattern) AND substr(pattern, ending, 1) != ']' LOOP
                    ending := ending + 1;
                END LOOP;
                IF ending > length(pattern) THEN RETURN false; END IF;
                members := substr(pattern, pos + 1, ending - pos - 1);
                expression := expression || '[' || replace(members, '\', '\\') || ']';
                pos := ending;
            ELSIF strpos('.+(){}|^$\]', ch) > 0 THEN expression := expression || '\' || ch;
            ELSE expression := expression || ch;
            END IF;
            pos := pos + 1;
        END LOOP;
        RETURN value ~ (expression || '\Z');
    END
    $fn$;
    CREATE FUNCTION not_glob_matches(value text, pattern text) RETURNS boolean LANGUAGE sql IMMUTABLE STRICT AS $fn$
        SELECT NOT nerve_pg.glob_matches(value, pattern)
    $fn$;
    CREATE OPERATOR ~~~ (LEFTARG = text, RIGHTARG = text, FUNCTION = glob_matches);
    CREATE OPERATOR !~~~ (LEFTARG = text, RIGHTARG = text, FUNCTION = not_glob_matches);

    ALTER TABLE plans ADD COLUMN summary TEXT;
    CREATE INDEX idx_plans_status_session_created ON plans(_scope, status, session_id, created_at);
    ALTER TABLE cron_logs ADD COLUMN records INTEGER;
    UPDATE cron_logs SET records = substring(output FROM '^([0-9]+)')::integer
        + COALESCE(substring(output FROM ' ingested, ([0-9]+) dropped by guardrail')::integer, 0)
        WHERE job_id LIKE 'source:%' AND output ~ '^[0-9]+ ingested';
    ALTER TABLE sessions ADD COLUMN trigger TEXT, ADD COLUMN origin_detail TEXT,
        ADD COLUMN title_source TEXT, ADD COLUMN fork_source_native_id TEXT,
        ADD COLUMN fork_source_turn_id TEXT;
    ALTER TABLE tasks ADD COLUMN created_session_id TEXT;
    CREATE INDEX idx_tasks_created_session ON tasks(_scope, created_session_id);
    CREATE INDEX idx_task_events_actor ON task_events(_scope, actor);
    UPDATE tasks t SET created_session_id = (
        SELECT e.actor FROM task_events e JOIN sessions s ON s._scope=e._scope AND s.id=e.actor
        WHERE e._scope=t._scope AND e.task_id=t.id AND e.from_status IS NULL
        ORDER BY e.created_at, e.id LIMIT 1
    );
    UPDATE sessions s SET title_source = (
        SELECT CASE WHEN json_extract(e.details, '$.reason') IS NULL THEN 'manual'
            WHEN json_extract(e.details, '$.reason') IN ('placeholder', 'generated')
                THEN json_extract(e.details, '$.reason') END
        FROM session_events e WHERE e._scope=s._scope AND e.session_id=s.id
            AND e.event_type='title_changed' ORDER BY e.id DESC LIMIT 1
    );

    -- Match v072's conservative fork recovery, including deleted/archived parents.
    FOR entry IN
        SELECT f.*, p.sdk_session_id AS parent_native_id
        FROM sessions f LEFT JOIN sessions p ON p._scope=f._scope AND p.id=f.parent_session_id
        WHERE f.parent_session_id IS NOT NULL AND COALESCE(f.source, '') != 'workflow'
          AND (f.id LIKE 'fork-%' OR f.forked_from_message IS NOT NULL
               OR json_extract(f.origin_detail, '$.fork_of') IS NOT NULL)
    LOOP
        SELECT json_extract(details, '$.sdk_session_id') INTO source_id
            FROM session_events WHERE _scope=entry._scope AND session_id=entry.id
                AND event_type='started' ORDER BY id LIMIT 1;
        source_id := COALESCE(NULLIF(source_id, ''), NULLIF(entry.parent_native_id, ''));
        IF source_id IS NULL THEN
            SELECT MIN(id) INTO first_event FROM session_events
                WHERE _scope=entry._scope AND session_id=entry.id;
            SELECT json_extract(details, '$.sdk_session_id') INTO source_id
                FROM session_events WHERE _scope=entry._scope AND session_id=entry.parent_session_id
                    AND event_type='started' AND json_extract(details, '$.sdk_session_id') IS NOT NULL
                    AND id < COALESCE(first_event, 4611686018427387904)
                ORDER BY id DESC LIMIT 1;
        END IF;
        IF source_id IS NULL OR (COALESCE(entry.sdk_session_id, '') != '' AND entry.sdk_session_id != source_id)
            OR EXISTS (SELECT 1 FROM session_events WHERE _scope=entry._scope AND session_id=entry.id
                AND event_type='started' AND json_extract(details, '$.sdk_session_id') != source_id)
        THEN CONTINUE; END IF;
        turn_id := NULL;
        IF entry.forked_from_message IS NOT NULL THEN
            IF entry.forked_from_message !~ '^[0-9]+$' THEN CONTINUE; END IF;
            SELECT native_turn_id INTO turn_id FROM messages
                WHERE _scope=entry._scope AND session_id=entry.parent_session_id
                    AND id::numeric >= entry.forked_from_message::numeric AND native_turn_id IS NOT NULL
                ORDER BY id LIMIT 1;
            IF turn_id IS NULL THEN
                SELECT native_turn_id INTO turn_id FROM messages
                    WHERE _scope=entry._scope AND session_id=entry.parent_session_id
                        AND id::numeric <= entry.forked_from_message::numeric AND native_turn_id IS NOT NULL
                    ORDER BY id DESC LIMIT 1;
            END IF;
            IF turn_id IS NULL THEN CONTINUE; END IF;
        END IF;
        UPDATE sessions SET fork_source_native_id=source_id, fork_source_turn_id=turn_id, sdk_session_id=NULL
            WHERE _scope=entry._scope AND id=entry.id;
    END LOOP;

    CREATE TABLE push_subscriptions (
        _scope TEXT NOT NULL DEFAULT current_setting('nerve.scope'),
        id BIGINT GENERATED BY DEFAULT AS IDENTITY,
        actor_id TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        p256dh TEXT NOT NULL,
        auth TEXT NOT NULL,
        level TEXT NOT NULL,
        user_agent TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        last_success_at TEXT,
        PRIMARY KEY (_scope, id), UNIQUE (_scope, endpoint),
        FOREIGN KEY (_scope, actor_id) REFERENCES actor_refs(_scope, id)
    );
    CREATE INDEX idx_push_subscriptions_actor ON push_subscriptions(_scope, actor_id);
    CREATE TABLE channel_reply_routes (
        _scope TEXT NOT NULL DEFAULT current_setting('nerve.scope'),
        channel TEXT NOT NULL,
        target TEXT NOT NULL,
        message_id TEXT NOT NULL,
        session_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (_scope, channel, target, message_id)
    );
    CREATE INDEX idx_channel_reply_routes_created ON channel_reply_routes(_scope, created_at);
    CREATE TABLE widget_states (
        _scope TEXT NOT NULL DEFAULT current_setting('nerve.scope'),
        session_id TEXT NOT NULL,
        widget_id TEXT NOT NULL,
        items TEXT NOT NULL,
        size INTEGER NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (_scope, session_id, widget_id),
        FOREIGN KEY (_scope, session_id) REFERENCES sessions(_scope, id) ON DELETE CASCADE
    );
    ALTER TABLE push_subscriptions ENABLE ROW LEVEL SECURITY;
    ALTER TABLE push_subscriptions FORCE ROW LEVEL SECURITY;
    CREATE POLICY scope ON push_subscriptions USING (_scope=current_setting('nerve.scope',true));
    ALTER TABLE channel_reply_routes ENABLE ROW LEVEL SECURITY;
    ALTER TABLE channel_reply_routes FORCE ROW LEVEL SECURITY;
    CREATE POLICY scope ON channel_reply_routes USING (_scope=current_setting('nerve.scope',true));
    ALTER TABLE widget_states ENABLE ROW LEVEL SECURITY;
    ALTER TABLE widget_states FORCE ROW LEVEL SECURITY;
    CREATE POLICY scope ON widget_states USING (_scope=current_setting('nerve.scope',true));
    UPDATE storage_version SET version=3;
END
$upgrade$;
COMMIT;
