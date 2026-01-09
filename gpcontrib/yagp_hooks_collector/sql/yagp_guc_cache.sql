--
-- Test GUC caching for query lifecycle consistency.
--
-- The extension logs SUBMIT and DONE events for each query.
-- GUC values that control logging (enable_utility, ignored_users_list, ...)
-- must be cached at SUBMIT time to ensure DONE uses the same filtering
-- criteria. Otherwise, a SET command that modifies these GUCs would
-- have its DONE event rejected, creating orphaned SUBMIT entries.
-- This is due to query being actually executed between SUBMIT and DONE.
CREATE EXTENSION yagp_hooks_collector;

CREATE OR REPLACE FUNCTION print_last_query_like(query_pattern text)
RETURNS TABLE(query_text text, query_status text) AS $$
    SELECT l.query_text, l.query_status
    FROM yagpcc.log l
    WHERE l.segid = -1 AND l.query_text LIKE query_pattern
    ORDER BY l.ccnt DESC
$$ LANGUAGE sql;

SET yagpcc.ignored_users_list TO '';
SET yagpcc.enable TO TRUE;
SET yagpcc.enable_utility TO TRUE;
SET yagpcc.logging_mode TO 'TBL';

-- SET below disables utility logging and DONE must still be logged.
SET yagpcc.enable_utility TO FALSE;
SELECT * FROM print_last_query_like('SET yagpcc.enable_utility%');

-- SELECT below adds current user to ignore list and DONE must still be logged.
-- start_ignore
SELECT set_config('yagpcc.ignored_users_list', current_user, false);
-- end_ignore
SELECT * FROM print_last_query_like('SELECT set_config%');

DROP FUNCTION print_last_query_like(text);
DROP EXTENSION yagp_hooks_collector;
RESET yagpcc.enable;
RESET yagpcc.enable_utility;
RESET yagpcc.ignored_users_list;
RESET yagpcc.logging_mode;
