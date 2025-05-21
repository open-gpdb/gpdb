-- Scenario to test SKIP_LOCKED usage

1: create table tp(id integer) 
DISTRIBUTED by (id)
PARTITION by RANGE (id)
(START (1) END (1000) EVERY (100), DEFAULT PARTITION extra);
1: insert into tp (select generate_series(1,1000));
1: begin;
1: lock table tp_1_prt_3;
2: analyze SKIP_LOCKED tp;
1: rollback;



1: create table t1(id integer) 
DISTRIBUTED by (id)
PARTITION by RANGE (id)
(START (1) END (1000) EVERY (100), DEFAULT PARTITION extra);
1: insert into tp (select generate_series(1,400));
1: begin;
1: lock table tp_1_prt_3;
2: analyze SKIP_LOCKED tp;
1: rollback;

1: analyze SKIP_LOCKED ROOTPARTITION tp;

1:drop table tp;
1:drop table tp1;