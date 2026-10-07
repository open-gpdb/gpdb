# CustomScan vocabulary — what everything is before you touch code

This is a short primer for readers who have never written a planner
extension for PostgreSQL / Greenplum before. It introduces the terms
the main [README.md](README.md) relies on: `Path`, `Plan`, `PlanState`,
`CustomScan`, hook, dispatch, locus. Skip it if you already know them.

Once you are comfortable with the ideas here, go back to
[README.md](README.md) and read it alongside
[custom_scan_test.c](custom_scan_test.c).

---

## 1. The three lives of a query

When the server receives `SELECT * FROM foo`, the data it reads passes
through three representations:

1. **Path** — a _candidate_ for how to compute the result.
   Several paths are generated per relation (seqscan, index scan,
   bitmap scan, …). Each carries cost estimates, and the planner picks
   the cheapest. A path is throwaway bookkeeping inside the planner.

2. **Plan** — the _winning_ path turned into an actual plan tree.
   This is the thing the executor runs. In GPDB this is also the thing
   that gets shipped to segments over the network.

3. **PlanState** — the _runtime_ form of a plan node. It holds
   open file descriptors, scan positions, intermediate buffers — all
   the "hot" state that only makes sense inside a running query.

So each executor node really exists in three forms: `Path → Plan →
PlanState`. `CustomScan` is no exception — it just lets _you_ supply
the three forms yourself instead of using the built-in ones.

---

## 2. What is a "custom scan"?

`CustomScan` is a hatch that PostgreSQL (and therefore GPDB) exposes to
extensions that treat own custom node as if it were a built-in one, as
long as it can be told how to plan it, how to execute it, and how to
explain it. It is filled in three small structs of callbacks, one per
life stage above:

- `CustomPathMethods` — how to turn own `CustomPath` into a `CustomScan` plan node
- `CustomScanMethods` — how to turn own `CustomScan` plan node into a `CustomScanState` at execution start
- `CustomExecMethods` — the usual Begin/Exec/End/ReScan/Explain callbacks

That is the entire contract.

---

## 3. What is a "hook"?

A hook is a global function pointer that PostgreSQL calls at a
well-defined spot in its own code. Every extension saves the previous
value, installs its own function, and thereby gets a chance to do
something at that spot. This is how extensions inject behavior without
patching the server.

The hook this extension uses is `set_rel_pathlist_hook`, which the
planner calls for every relation _after_ it has generated the standard
paths, so extensions can add more.

---

## 4. What is "dispatch" in GPDB?

In Greenplum a query is planned once on the **coordinator** (also
called QD — "query dispatcher") and then sent to every **segment**
(the data-holding workers, QE — "query executor") for parallel
execution. "Dispatch" is the name of that send step. The plan itself
travels as a text blob: the QD serializes it with `nodeToString()`,
the segment rebuilds it with `stringToNode()`, then runs it.

This matters because anything we want the segments to see — file
paths, tuning knobs, flags — has to survive that round trip.

---

## 5. What is "locus"?

Vanilla PostgreSQL does not care where the data physically lives.
Greenplum does: a relation can be hash-distributed, randomly
distributed, replicated, or live only on the coordinator. The planner
tracks this for every `Path` in a field called `locus`. When two paths
with different loci are combined, the planner inserts a `Motion` node
that physically moves tuples across the network.

Your own custom path must declare its locus so the planner can place
motions correctly. The easiest working default is to copy the locus
from the existing cheapest path — this extension does exactly that.
See section 1.3 of [README.md](README.md) for the concrete table of
`CdbPathLocus_Make*` choices.

---

Ready? Open [README.md](README.md) and start at section 1.
