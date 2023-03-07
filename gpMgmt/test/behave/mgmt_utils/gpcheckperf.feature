@gpcheckperf
Feature: Tests for gpcheckperf

  @concourse_cluster
  Scenario: gpcheckperf runs disk and memory tests
    Given the database is running
    When  the user runs "gpcheckperf -h cdw -h sdw1 -d /data/gpdata/ -r ds"
    Then  gpcheckperf should return a return code of 0
    And   gpcheckperf should print "disk write tot bytes" to stdout

  @concourse_cluster
  Scenario: gpcheckperf runs runs sequential network test
    Given the database is running
    When  the user runs "gpcheckperf -h cdw -h sdw1 -d /data/gpdata/ -r n"
    Then  gpcheckperf should return a return code of 0
    And   gpcheckperf should print "avg = " to stdout
    And   gpcheckperf should not print "NOTICE: -t is deprecated " to stdout

  Scenario: gpcheckperf runs with -S option and prints a warning message
    Given the database is running
    When  the user runs "gpcheckperf -h localhost -r d -d /tmp -S 1GB"
    Then  gpcheckperf should return a return code of 0
    And   gpcheckperf should print "\[Warning] Using 1073741824 bytes for disk performance test. This might take some time" to stdout

  Scenario: gpcheckperf errors out when invalid value is passed to the -S option
    Given the database is running
    When  the user runs "gpcheckperf -h localhost -r d -d /tmp -S abc"
    Then  gpcheckperf should return a return code of 1
