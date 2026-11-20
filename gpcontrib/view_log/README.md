# view_log

The extension logs all references to database views in Greenplum queries.

## Limitations
- **Master Only**: Logs are written only on the Coordinator/Master node.
- **Presence-based**: A view is logged if it appears in the query's Range Table (`RTE_RELATION`) during the planning phase. This means the view is logged even if the optimizer later removes it via partition pruning or constant folding, provided it was initially part of the logical plan.
- **No Execution Guarantee**: Logging occurs at the `planner` hook stage. It confirms the view was referenced, not necessarily that data was fetched from its underlying tables.

## Installation

Load the extension in the current session:

```
LOAD '$libdir/view_log';
```
