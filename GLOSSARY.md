# NSW PSN / AusAware

The domain vocabulary for this project. One definition per term — what it **is**, not what it does. Keep it free of implementation detail.

## Language

### Radio reception

**Logical call**:
One transmission on the network, regardless of how many nodes received it. Identified by `logical_call_id` (radio) / `logical_id` (pager).
_Avoid_: call (bare), transmission.

**Reception**:
One *observed* logical call — a single (logical call, node, site) tuple, optionally scoped to a talkgroup. Receptions are counted, never multiplied: the same logical call heard by three nodes is three receptions of one logical call.
_Avoid_: call, hit, decode.

**Rollup edge**:
The time seam that divides completed hours from the in-progress hour. Completed hours are served from the hourly rollup tables; the current hour (and any not-yet-rolled tail) is served from the detail events table. Every fleet-wide reception query is a union across this seam.
_Avoid_: cutoff, boundary.

**Window**:
The look-back span of a reception query: `24h`, `7d`, `30d`, or `all`. For radio, `all` resolves to 30 days, because every radio rollup derives from the detail table, which is pruned at 30 days (a capped window, flagged to the caller).
_Avoid_: range, period.
