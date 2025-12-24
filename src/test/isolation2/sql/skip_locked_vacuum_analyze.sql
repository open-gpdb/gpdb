-- Scenario to test SKIP_LOCKED usage

1: analyze SKIP_LOCKED tp;

1: create extension gp_aux_catalog;
1: alter extension gp_aux_catalog update to '1.1';

1: create table tp(id integer) 
DISTRIBUTED by (id)
PARTITION by RANGE (id)
(START (1) END (1000) EVERY (100), DEFAULT PARTITION extra);
1: insert into tp select generate_series(1,550);
1: insert into tp select generate_series(640,660);
1: begin;
1: lock table tp_1_prt_3;

2: analyze SKIP_LOCKED tp;

1: rollback;

1:drop table tp;
