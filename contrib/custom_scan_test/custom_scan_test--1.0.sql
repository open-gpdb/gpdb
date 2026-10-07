/* contrib/custom_scan_test/custom_scan_test--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION custom_scan_test" to load this file. \quit

-- The extension auto-loads via shared_preload_libraries or LOAD.
-- No SQL objects needed; the custom scan is injected via planner hook.
