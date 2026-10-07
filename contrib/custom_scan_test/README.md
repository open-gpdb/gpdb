# custom_scan_test

Teaching-quality demo extension for the **CustomScan** infrastructure
in Greenplum. Serves double duty:

- as a **reference** for how to build any extension that plugs its own
  scan/join nodes into the planner and executor, and
- as a **worked example** that reads rows from metadata-routed CSV
  files instead of (or alongside) the underlying heap.

## Documentation, split by concern

| File                                           | Read it when…                                                                                  |
|------------------------------------------------|-----------------------------------------------------------------------------------------------|
| [README-intro.md](README-intro.md)             | You have never touched the PG/GPDB planner. Short primer that defines `Path`, `Plan`, `PlanState`, hook, dispatch, locus. |
| [README-howto.md](README-howto.md)             | You want to **build your own** CustomScan extension. Generic, framework-style walkthrough; uses this extension only for code pointers. |
| [README-toy.md](README-toy.md)                 | You want to understand **this specific** extension — its CSV modes, locus demo, per-segment metadata routing, and how to run it. |

## Source map

| File                                                       | Role                                                         |
|------------------------------------------------------------|--------------------------------------------------------------|
| [custom_scan_test.c](custom_scan_test.c)                   | All the C code: planner hook, CustomPath/Scan/State methods, CSV reader, SPI metadata lookup. |
| [custom_scan_test--1.0.sql](custom_scan_test--1.0.sql)     | Metadata schema + `register_csv()` / `unregister_csv()`.      |
| [custom_scan_test.control](custom_scan_test.control)       | Extension metadata for `CREATE EXTENSION`.                    |
| [Makefile](Makefile)                                       | Standard PGXS/contrib build + `REGRESS` registration.         |
| [sample.csv](sample.csv), [sample_seg0.csv](sample_seg0.csv), [sample_seg1.csv](sample_seg1.csv), [sample_seg2.csv](sample_seg2.csv) | Example data used by the toy demo.              |
| [toy_demo.sql](toy_demo.sql)                               | Manual narrated scenario; human-readable companion to the test. |
| [sql/custom_scan_test.sql](sql/custom_scan_test.sql)       | Regression test input.                                        |
| [expected/custom_scan_test.out](expected/custom_scan_test.out) | Regression test golden output.                            |

## Quick start

```sh
cd contrib/custom_scan_test
make install             # build and install the extension
make installcheck        # run the regression test (needs a running cluster)
```

Then, for an interactive look, open [toy_demo.sql](toy_demo.sql) and
follow the prompts — or jump straight to [README-toy.md](README-toy.md).
