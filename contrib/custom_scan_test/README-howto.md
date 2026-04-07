# How to build an extension with CustomScan nodes in Greenplum

A recipe for writing your own extension that injects its own scan (or
join, or aggregate) nodes into the planner and executor. The
vocabulary this document uses (`Path` / `Plan` / `PlanState`, hook,
dispatch, locus) is defined in [README-intro.md](README-intro.md).

All code pointers below refer to the worked example that lives in
this directory, [custom_scan_test.c](custom_scan_test.c). Treat it as
"this is what a minimal answer looks like"; replace the mechanics
(heap vs CSV, etc.) with whatever your data source is.

Contents:

1. [Skeleton of a CustomScan extension](#1-skeleton-of-a-customscan-extension)
2. [Passing data into your node](#2-passing-data-into-your-node)
3. [Running on segments](#3-running-on-segments)
4. [Troubleshooting](#4-troubleshooting)

---

## 1. Skeleton of a CustomScan extension

The overall picture — each arrow is a callback _you_ write:

```
  set_rel_pathlist_hook     ← planner calls this for every relation
            │
            ▼
     CustomPath             ← your candidate (cost-estimated)
            │
            ▼ PlanCustomPath()
     CustomScan             ← plan node that goes in the final tree
            │
            ▼ dispatch to segments  (nodeToString / stringToNode)
            │
            ▼ CreateCustomScanState()
     CustomScanState        ← runtime state; has Begin/Exec/End/ReScan/Explain
```

### 1.1. Files your extension needs

Same layout as any contrib extension:

| File                | Role                                                                     |
|---------------------|--------------------------------------------------------------------------|
| `myext.control`     | Metadata read by `CREATE EXTENSION` (version, module path).              |
| `myext--1.0.sql`    | SQL script for `CREATE EXTENSION`. May be empty if the node is injected purely from C. |
| `myext.c`           | Hooks, callback structs, GUC definitions.                                |
| `Makefile`          | Standard contrib/PGXS boilerplate.                                       |

### 1.2. What `_PG_init()` must do

`_PG_init()` is the entry point PostgreSQL calls when it loads your
`.so`. It runs once per backend process. Do three things, in this
order:

```c
void
_PG_init(void)
{
    /*
     * 1. Register your CustomScanMethods under a unique name.
     *    Segments need this so stringToNode() can rebuild your node
     *    from the dispatched plan; without it the query fails with
     *    "unregistered custom scan method".
     */
    RegisterCustomScanMethods(&my_scan_methods);

    /* 2. Define any GUC variables. */
    DefineCustomBoolVariable("myext.enabled", ...);

    /*
     * 3. Install your planner hook.  Save the previous value and
     *    chain to it — other extensions may have their own hook.
     */
    prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
    set_rel_pathlist_hook = my_set_rel_pathlist;
}
```

"Chain to the previous hook" is the etiquette of hook writing. Never
overwrite a hook pointer without saving and calling the old value,
otherwise you silently break every other extension that wanted the
same hook.

Reference: [custom_scan_test.c:_PG_init](custom_scan_test.c).

### 1.3. Planner hook → CustomPath

The planner calls `set_rel_pathlist_hook` after generating the
standard paths (SeqScan, IndexScan, …) for a relation. This is your
chance to add your own:

```c
/* Allocate a CustomPath that points at your CustomPathMethods. */
cpath = create_customscan_path(root, rel, NIL, 0, &my_path_methods);

/*
 * Fill in cost fields.  The planner will compare to the other paths
 * and pick the cheapest.
 */
cpath->path.rows        = cheapest->rows;
cpath->path.startup_cost = cheapest->startup_cost;
cpath->path.total_cost  = cheapest->total_cost * 0.99;

/* Declare where rows physically live — see below. */
cpath->path.locus = cheapest->locus;

add_path(rel, (Path *) cpath);
```

> **GPDB gotcha:** forgetting `cpath->path.locus` is the classic
> first mistake. The planner will crash or build a wrong motion tree.
> Always set it, even if you just copy from another path.

#### What value to put in `locus`?

`CdbPathLocus` (see `src/include/cdb/cdbpathlocus.h`) describes where
the rows physically live and who should read them. You construct it
with one of the `CdbPathLocus_Make*` macros. Pick based on your data
source:

| Situation                                                | What to write                                              |
|----------------------------------------------------------|------------------------------------------------------------|
| Your node wraps an existing relation                     | `cpath->path.locus = cheapest->locus;`                     |
| Source lives only on the coordinator (QD-only)           | `CdbPathLocus_MakeEntry(&cpath->path.locus);`              |
| Source is locally available on every segment             | `CdbPathLocus_MakeSegmentGeneral(&cpath->path.locus, N);`  |
| Source is logically available everywhere (pure/virtual)  | `CdbPathLocus_MakeGeneral(&cpath->path.locus, N);`         |
| Each segment reads its own slice; no known partition key | `CdbPathLocus_MakeStrewn(&cpath->path.locus, N);`          |
| Source is hash-partitioned by a known key                | `CdbPathLocus_MakeHashed(&cpath->path.locus, distkey, N);` |

`N` is the number of segments — typically `getgpsegmentCount()` or
`cheapest->locus.numsegments`. If you pick the wrong locus, GPDB
does not magically fix it — you just get duplicated or missing rows.

### 1.4. Path → Plan (`PlanCustomPath`)

Once the planner has chosen your path, it calls your `PlanCustomPath`
callback to produce a real plan node:

```c
static Plan *
MyPlanCustomPath(PlannerInfo *root, RelOptInfo *rel,
                 CustomPath *best_path, List *tlist,
                 List *clauses, List *custom_plans)
{
    CustomScan *cscan = makeNode(CustomScan);

    cscan->scan.plan.targetlist = tlist;
    cscan->scan.plan.qual       = extract_actual_clauses(clauses, false);
    cscan->scan.scanrelid       = rel->relid;
    cscan->methods              = &my_scan_methods;

    /* Payload for segments — see section 2. */
    cscan->custom_private       = NIL;

    return (Plan *) cscan;
}
```

Field meanings:

- `targetlist` — columns the node must emit.
- `qual` — filter predicates. If you delegate to `ExecScan()` in your
  executor callbacks, you get qual checking for free.
- `scanrelid` — range-table index of the scanned relation; the
  executor will open it and give you `ss_currentRelation`.

### 1.5. Plan → State (`CreateCustomScanState`)

At execution start, for each `CustomScan` node the executor calls
your `CreateCustomScanState`. It must return a struct whose **first
field** is `CustomScanState`:

```c
typedef struct MyCustomScanState
{
    CustomScanState  css;    /* MUST be the first field */
    /* ...your own runtime state... */
} MyCustomScanState;
```

This first-field rule lets the executor cast your struct freely. It
is the PostgreSQL convention for node "inheritance".

### 1.6. Exec callbacks: Begin / Exec / End / ReScan / Explain

These are the familiar executor callbacks, wired via
`CustomExecMethods`:

- `BeginCustomScan` — open whatever you need to read (files, network
  handles, heap scan). Runs once per segment.
- `ExecCustomScan` — produce the next tuple. The easy path is to
  delegate to `ExecScan()` with an access-method callback that simply
  returns the next raw tuple; `ExecScan()` handles qual checks and
  projection for you.
- `EndCustomScan` — close anything you opened in `BeginCustomScan`.
- `ReScanCustomScan` — reset scan position (`rewind`, `heap_rescan`).
- `ExplainCustomScan` — emit `ExplainProperty*` lines so `EXPLAIN
  VERBOSE` shows what your node is doing.

---

## 2. Passing data into your node

The most common beginner question: "I generated my path on the QD
and it knows _X_ — how do I tell the executor about _X_, especially
when _X_ has to travel to every segment?"

Use the "custom_\*" fields of `CustomScan`. Their contents are
copied by `copyObject()`, serialized by `nodeToString()`, and
rebuilt on segments by `stringToNode()`:

| Field                | Purpose                                                                                | Holds                                                                          |
|----------------------|----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| `custom_private`     | Arbitrary per-node metadata.                                                           | `List` of constant-like nodes: `Integer`, `Float`, `String`, `Const`, nested `List`, `Bitmapset`. |
| `custom_exprs`       | Expressions (`Expr *`) that must be rewritten by `setrefs`/`fix_expr_ref_*`.           | `List` of `Expr *`.                                                            |
| `custom_scan_tlist`  | A "virtual" targetlist if the node emits different columns than the scanrel.           | `List` of `TargetEntry`.                                                       |
| `custom_plans`       | Child `Plan` nodes if your node has sub-plans.                                         | `List` of `Plan *`.                                                            |
| `custom_relids`      | Bitmapset of relids the node covers (used by join planning).                           | any `Bitmapset`.                                                               |

Most extensions only ever use `custom_private`.

### Worked example: storing a string in `custom_private`

**In your planning code (on the QD):**

```c
cscan->custom_private = list_make1(makeString(pstrdup(some_string)));
```

**In `BeginCustomScan` (on every segment):**

```c
CustomScan *cscan = (CustomScan *) node->ss.ps.plan;

if (cscan->custom_private != NIL)
{
    char *s = strVal(linitial(cscan->custom_private));
    /* ...use it... */
}
```

That's it. The `List(String)` you built on the QD is reborn on every
segment with exactly the same contents.

For a multi-element example (mode + int + list of per-segment
entries), see
[`TestBeginCustomScan`](custom_scan_test.c) in this extension — it
unpacks a 3-element list holding an integer mode, a column count,
and a `List-of-(Integer, String)` routing table.

---

## 3. Running on segments

"Passing" data is half the story; the other half is running with it
on the segments. The lifecycle to keep in mind:

```
   QD side (coordinator):
     ├─ set_rel_pathlist_hook           ← your hook runs
     ├─ CustomPath created
     ├─ PlanCustomPath                  ← you write custom_private here
     └─ nodeToString(plan)              ← the plan becomes a big string
                       │
                       │  TCP dispatch
                       ▼
   QE side (every segment):
     ├─ stringToNode(plan_string)       ← plan comes back to life
     ├─ CreateCustomScanState           ← your method is looked up by name
     ├─ BeginCustomScan                 ← you read custom_private here
     ├─ ExecCustomScan ... (rows)
     └─ EndCustomScan
```

### 3. `EXPLAIN ANALYZE`

`EXPLAIN (ANALYZE, VERBOSE)` in GPDB collects per-segment statistics
and renders everything you print from `ExplainCustomScan`. That's an
easy way to confirm:

- whether `custom_private` actually reached the segments (print it);
- which path each segment took (print a mode / source tag);
- how many rows each segment produced.

Example:

```c
ExplainPropertyText("Custom Scan Type", "MyCustomScan", es);
ExplainPropertyText("Source", mystate->source_name, es);
ExplainPropertyInteger("Partition", mystate->partition_no, es);
```

A VERBOSE plan will show these lines for every segment slice.

---
