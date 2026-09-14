# Test the vacuum statistics collected for tables, indexes and databases.
#
# The scenarios follow the regression tests of the upstream "Vacuum
# statistics" patch
# (https://www.postgresql.org/message-id/70e1cca9-ff89-4e76-a611-d38bcc0e14ad%40yandex.ru),
# adapted to the asynchronous statistics collector by polling for the
# expected state.  The node runs in utility mode, so the local
# pg_stat_vacuum_* views are used.  On top of the upstream scenarios this
# also checks that the statistics survive a clean restart of the cluster
# and are reset by crash recovery, like the rest of the collected
# statistics.
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 13;

my $node = get_new_node('vacuum_stats');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->append_conf('postgresql.conf', 'track_counts = on');
# The node runs standalone in utility mode, so there is no dispatcher to
# advance the distributed oldest xmin; run in maintenance mode, where
# GetOldestXmin() does not consult the distributed log and vacuum can
# remove tuples.
$node->append_conf('postgresql.conf', 'maintenance_mode = on');
# freeze aggressively, so that every vacuum has to scan the whole
# relation and is counted as a wraparound-driven one
$node->append_conf('postgresql.conf', 'vacuum_freeze_min_age = 0');
$node->append_conf('postgresql.conf', 'vacuum_freeze_table_age = 0');
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION vacuum_stats');
$node->safe_psql('postgres',
	'CREATE TABLE vestat (x int primary key) '
	  . 'WITH (autovacuum_enabled = off, fillfactor = 10)');
$node->safe_psql('postgres',
	'INSERT INTO vestat SELECT g FROM generate_series(1, 10000) g');
$node->safe_psql('postgres', 'ANALYZE vestat');

my $tab_counters =
	'SELECT tuples_deleted, dead_tuples, pages_deleted, dead_pages, '
  . 'pages_frozen, pages_all_visible, rev_all_frozen_pages, '
  . 'rev_all_visible_pages, '
  . 'wraparound_vacuum_count, total_time '
  . "FROM pg_stat_vacuum_tables WHERE relname = 'vestat'";
my $idx_counters =
	'SELECT tuples_deleted, pages_deleted, dead_pages, total_time '
  . "FROM pg_stat_vacuum_indexes WHERE indexrelname = 'vestat_pkey'";

# Before the first vacuum execution the extended stats view is empty.
my $result = $node->safe_psql('postgres',
	'SELECT tuples_deleted = 0 AND pages_deleted = 0 AND pages_frozen = 0 '
	  . 'AND pages_all_visible = 0 AND wraparound_vacuum_count = 0 '
	  . 'AND total_time = 0 '
	  . "FROM pg_stat_vacuum_tables WHERE relname = 'vestat'");
is($result, 't', 'table statistics are empty before the first vacuum');

$node->safe_psql('postgres', 'DELETE FROM vestat WHERE x % 2 = 0');
$node->safe_psql('postgres', 'VACUUM vestat');

# Vacuum removed tuples, but the table was not truncated.
ok( $node->poll_query_until(
		'postgres',
		'SELECT tuples_deleted > 0 AND pages_frozen > 0 '
		  . 'AND pages_all_visible > 0 '
		  . 'AND pages_deleted = 0 AND total_time > 0 '
		  . "FROM pg_stat_vacuum_tables WHERE relname = 'vestat'"),
	'vacuum removed tuples of the table and froze pages');

# vacuum_freeze_table_age is 0, so every run scans the whole relation and
# is counted as a wraparound-driven vacuum.
ok( $node->poll_query_until(
		'postgres',
		'SELECT wraparound_vacuum_count > 0 '
		  . "FROM pg_stat_vacuum_tables WHERE relname = 'vestat'"),
	'the full-table vacuum is counted as a wraparound one');

# Vacuum removed index entries, but no index pages could be deleted yet
# because every other key remains.
ok( $node->poll_query_until(
		'postgres',
		'SELECT tuples_deleted > 0 AND pages_deleted = 0 AND total_time > 0 '
		  . "FROM pg_stat_vacuum_indexes WHERE indexrelname = 'vestat_pkey'"),
	'vacuum removed index entries');

# Take a baseline once the counters have settled.
$node->safe_psql('postgres', 'SELECT pg_sleep(1)');
my ($tab_deleted, $tab_pages_deleted) = split /\|/,
  $node->safe_psql('postgres',
	'SELECT tuples_deleted, pages_deleted '
	  . "FROM pg_stat_vacuum_tables WHERE relname = 'vestat'");
my ($idx_deleted, $idx_pages_deleted) = split /\|/,
  $node->safe_psql('postgres',
	'SELECT tuples_deleted, pages_deleted '
	  . "FROM pg_stat_vacuum_indexes WHERE indexrelname = 'vestat_pkey'");

$node->safe_psql('postgres', 'DELETE FROM vestat');
$node->safe_psql('postgres', 'VACUUM vestat');

# The emptied pages must have been truncated away from the heap and
# deleted from the index.
ok( $node->poll_query_until(
		'postgres',
		"SELECT pages_deleted > $tab_pages_deleted "
		  . "AND tuples_deleted > $tab_deleted "
		  . "FROM pg_stat_vacuum_tables WHERE relname = 'vestat'"),
	'the emptied heap pages were truncated away');

ok( $node->poll_query_until(
		'postgres',
		"SELECT pages_deleted > $idx_pages_deleted "
		  . "AND tuples_deleted > $idx_deleted "
		  . "FROM pg_stat_vacuum_indexes WHERE indexrelname = 'vestat_pkey'"),
	'the emptied index pages were deleted');

# VACUUM FULL doesn't report to the stats collector, so no advancement of
# the statistics is detected here.
$node->safe_psql('postgres', 'SELECT pg_sleep(1)');
my $tab_before = $node->safe_psql('postgres', $tab_counters);
my $idx_before = $node->safe_psql('postgres', $idx_counters);

$node->safe_psql('postgres',
	'INSERT INTO vestat SELECT g FROM generate_series(1, 10000) g');
$node->safe_psql('postgres', 'DELETE FROM vestat WHERE x % 2 = 0');
$node->safe_psql('postgres', 'VACUUM FULL vestat');
$node->safe_psql('postgres', 'SELECT pg_sleep(1)');

is($node->safe_psql('postgres', $tab_counters),
	$tab_before, 'VACUUM FULL does not advance the table statistics');
is($node->safe_psql('postgres', $idx_counters),
	$idx_before, 'VACUUM FULL does not advance the index statistics');

# A plain vacuum marks the rewritten pages all-visible again.
my ($all_visible, $rev_all_visible) = split /\|/,
  $node->safe_psql('postgres',
	'SELECT pages_all_visible, rev_all_visible_pages '
	  . "FROM pg_stat_vacuum_tables WHERE relname = 'vestat'");

$node->safe_psql('postgres', 'VACUUM vestat');

ok( $node->poll_query_until(
		'postgres',
		"SELECT pages_all_visible > $all_visible "
		  . "FROM pg_stat_vacuum_tables WHERE relname = 'vestat'"),
	'vacuum marked the pages all-visible again');

# Ordinary DML revokes the all-visible status of the pages it touches;
# this is delivered with the regular relation statistics.  There is no
# all-frozen bit in the visibility map in this release, so the
# corresponding counter stays zero.
$node->safe_psql('postgres', 'UPDATE vestat SET x = x + 20000');

ok( $node->poll_query_until(
		'postgres',
		"SELECT rev_all_visible_pages > $rev_all_visible "
		  . 'AND rev_all_frozen_pages = 0 '
		  . "FROM pg_stat_vacuum_tables WHERE relname = 'vestat'"),
	'the backend revoked the all-visible status of the updated pages');

# The database-level counters accumulate the per-relation ones.
ok( $node->poll_query_until(
		'postgres',
		'SELECT tuples_deleted > 0 AND wraparound_vacuum_count > 0 '
		  . "AND total_time > 0 "
		  . "FROM pg_stat_vacuum_database WHERE datname = 'postgres'"),
	'the database-level statistics accumulate the vacuum counters');

# The statistics survive a clean restart: the collector saves them to the
# permanent statistics files when it exits.
$node->safe_psql('postgres', 'SELECT pg_sleep(1)');
$tab_before = $node->safe_psql('postgres', $tab_counters);
$node->restart;
is($node->safe_psql('postgres', $tab_counters),
	$tab_before, 'vacuum statistics survive a clean restart');

# Like the rest of the collected statistics, they are reset by crash
# recovery.
$node->stop('immediate');
$node->start;
ok( $node->poll_query_until(
		'postgres',
		'SELECT tuples_deleted = 0 AND total_time = 0 '
		  . "FROM pg_stat_vacuum_tables WHERE relname = 'vestat'"),
	'vacuum statistics are reset by crash recovery');

$node->stop;
