-- Test UDS socket
CREATE EXTENSION yagp_hooks_collector;

-- Start receiver with 3 second timeout, args are <socket> <output> <timeout_sec>
\! python3 sql/uds_test_receiver.py /tmp/yagpcc_test.sock /tmp/yagpcc_result.txt 3 &

-- Configure extension to send via UDS
SET yagpcc.uds_path TO '/tmp/yagpcc_test.sock';
SET yagpcc.ignored_users_list TO '';
SET yagpcc.enable TO TRUE;
SET yagpcc.logging_mode TO 'UDS';

\! sleep 1

-- Trigger sending
SELECT 1;

-- Wait for 4s and show results
\! sleep 4 && cat /tmp/yagpcc_result.txt

DROP EXTENSION yagp_hooks_collector;
RESET yagpcc.uds_path;
RESET yagpcc.ignored_users_list;
RESET yagpcc.enable;
RESET yagpcc.logging_mode;
