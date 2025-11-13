# Test for fixing timeline collision when restore_command is not written
# into the generated recovery.conf during recovery, resulting in the
# promoting node not seeing a newer timeline ID in the archive. Now
# restore_command is written to recovery.conf and sourced from the
# primary's restore_command_hint GUC during recovery.

# Steps:
# 1. Create master + standby, promote standby → timeline 2.
# 2. Stop standby, clean its datadir, rebuild from master.
# 3. Standby now on timeline 1, but timeline 2 exists in shared archive.
# 4. Promote standby again → should fetch timeline 2 from archive using
#    restore_command and jump to timeline 3.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 6;
use File::Copy;
use File::Path qw(rmtree);

# Initialize master with archiving and streaming
my $node_master = get_new_node('master');
$node_master->init(has_archiving => 1, allows_streaming => 1);

# Declare master’s archive for both master and standby
my $archive_dir = $node_master->archive_dir;

# Append restore_command_hint to master's postgresql.conf
my $path = TestLib::perl2host($archive_dir);
my $restore_cmd = qq{cp "$path/%f" "%p"};

$node_master->append_conf('postgresql.conf',
    "restore_command_hint = '$restore_cmd'\n");

$node_master->start;

# Create test data
$node_master->safe_psql('postgres', "CREATE TABLE test_tbl AS SELECT 1 AS id");

# Take backup for standby
my $backup_name = 'my_backup';
$node_master->backup($backup_name);

# Initialize standby from backup
my $node_standby = get_new_node('standby');
$node_standby->init_from_backup($node_master, $backup_name,
    has_streaming => 1,
    has_restoring => 1);

# Enable archiving on standby for timeline history archival
my $archive_cmd = qq{cp "%p" "$archive_dir/%f"};

$node_standby->append_conf('postgresql.conf', qq{
archive_mode = on
archive_command = '$archive_cmd'
});

# Disable hot standby as it is not supported
$node_standby->append_conf('postgresql.conf', "hot_standby = off\n");

# Start standby and wait for it to catch up
$node_standby->start;
my $target_lsn = $node_master->lsn('insert');
$node_master->wait_for_catchup($node_standby, 'replay', $target_lsn);

# First promotion: standby moves to timeline 2
$node_standby->promote;
$node_standby->poll_query_until('postgres', "SELECT NOT pg_is_in_recovery()")
  or die "Timed out waiting for first promotion";

# Force checkpoint and wait for timeline history file to be archived
$node_standby->safe_psql('postgres', "CHECKPOINT");

# Verify 00000002.history is archived
my $standby_archive = $node_standby->archive_dir;
$node_standby->poll_query_until('postgres',
    "SELECT size IS NOT NULL FROM pg_stat_file('pg_xlog/archive_status/00000002.history.done', true);")
  or die "Timed out waiting for 00000002.history to be archived";

ok(-f "$archive_dir/00000002.history",
   'timeline 2 history file archived after first promotion');

# Crash standby
$node_standby->stop('immediate');

# Rebuild standby from master using pg_basebackup
my $standby_datadir = $node_standby->data_dir;
File::Path::rmtree($standby_datadir);

$ENV{PGOPTIONS} = '-c gp_session_role=utility';
command_ok(['pg_basebackup',
            '-D', $standby_datadir,
            '-h', $node_master->host,
            '-p', $node_master->port,
            '--target-gp-dbid', '123',
            '-x',
            '--write-recovery-conf'],
           'pg_basebackup standby from master');

# Verify recovery.conf contains restore_command
my $recovery_conf = "$standby_datadir/recovery.conf";
ok(-f $recovery_conf, 'recovery.conf exists after pg_basebackup');

my $recovery_conf_content = slurp_file($recovery_conf);
like($recovery_conf_content, qr/restore_command\s*=/,
    'recovery.conf contains restore_command');

# Restore configuration
$node_standby->append_conf('postgresql.conf', "hot_standby = off\n");
$node_standby->append_conf('postgresql.conf', qq{
archive_mode = on
archive_command = '$archive_cmd'
});

# Start standby (now back on timeline 1)
$node_standby->start;

# Kill master and promote standby again
$node_master->stop('immediate');

# Promote using pg_ctl directly since PostgresNode->promote() verification fails in mirror mode
# (after pg_basebackup we have gp_contentid=0 and it affects connection acceptance).
# Verify via logs.
my $pgdata = $node_standby->data_dir;
my $logfile = $node_standby->logfile;
print "### Promoting node \"" . $node_standby->name . "\"\n";
TestLib::system_or_bail('pg_ctl', '-D', $pgdata, '-l', $logfile, 'promote');

# Wait for second promotion to complete
my $max_wait = 60;
my $waited = 0;
my $promotions = 0;

while ($waited < $max_wait) {
    my $log_content = slurp_file($logfile);
    my @promotion_matches = $log_content =~ /database system is ready to accept connections/g;
    $promotions = scalar @promotion_matches;

    last if $promotions >= 2;

    sleep(1);
    $waited++;
}

die "Second promotion did not complete in time (found $promotions ready messages)" if $promotions < 2;

# Verify timeline 2 was selected only once and currently active timeline is 3
my $log_content = slurp_file($logfile);
my @timeline_2_matches = $log_content =~ /selected new timeline ID: 2/g;
is(scalar @timeline_2_matches, 1, 'timeline 2 selected once');

my @timeline_3_matches = $log_content =~ /selected new timeline ID: 3/g;
is(scalar @timeline_3_matches, 1, 'timeline 3 selected once');

