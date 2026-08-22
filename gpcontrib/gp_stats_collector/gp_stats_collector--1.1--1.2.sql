/* gp_stats_collector--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_stats_collector UPDATE TO '1.2'" to load this file. \quit

-- EXPLAIN (FORMAT JSON) payloads; appended at the end, order must match LogSchema.h.
ALTER TABLE gpsc.__log ADD COLUMN plan_json text;
ALTER TABLE gpsc.__log ADD COLUMN analyze_json text;

-- Recreate the view so that it picks up the new columns.
DROP VIEW gpsc.log;
CREATE VIEW gpsc.log AS
  SELECT * FROM gpsc.__log -- master
  UNION ALL
  SELECT * FROM gp_dist_random('gpsc.__log') -- segments
  ORDER BY tmid, ssid, ccnt;
