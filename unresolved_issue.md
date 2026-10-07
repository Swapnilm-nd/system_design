# DIS Sliding-Window Split-Merge: Unresolved Issues

## Open

### C — `redis_key_ttl_seconds` / blanket key TTL removed from the doc, not yet resolved

**Section:** previously §2.5 ("Blanket key TTL") and its parameter-table row, both now removed from
[`dis_sliding_window_split_merge_design.md`](dis_sliding_window_split_merge_design.md).

**What it was:** a single, generous TTL applied to every Redis key type in the design
(`dis_embed_list`, `dis_saved_sessions`, `dis_session_meta`, `dis_active_devices`), not the real
cleanup mechanism (that's `sessions_eviction`'s normal finalize/evacuate flow, §5) — purely a leak
backstop, so a bug or an unswept edge case can't leak Redis memory forever.

**Status:** removed from the design doc for now, at the user's request, rather than left in as an
unresolved parameter. Tracked here so the backstop isn't forgotten — worth revisiting before
implementation, since without it, a bug or edge case that leaves a key behind has no safety net at
all.

---

Everything else raised so far — the original review pass plus a follow-up fresh pass over the
consolidated document — was resolved and applied directly to
[`dis_sliding_window_split_merge_design.md`](dis_sliding_window_split_merge_design.md).

---

## Second review pass (post-consolidation)

Two issues surfaced re-reading the fully consolidated doc fresh, rather than from memory of the
original review — both resolved and applied immediately.

### A — §2.3's `start_dct`/`end_dct` update-trigger rule contradicted §4.3.2

**Problem:** §2.3 stated these fields "update on every pop-type event only... never on
assignment." But §4.3.2's historical-attach case extends `start_dct`/`end_dct` in the *same step*
that bumps `last_updated_at` — which the doc itself calls an assignment event. Direct
self-contradiction.

**Resolution:** restated the actual rule — these fields change only when the *saved dct list
itself* changes, which happens via a live-list pop (§4.2 step 1) **or** a historical-path write
(§4.3.2), since historical writes go straight to the saved list with no separate pop step at all.
For historical writes, "assignment" and "the saved list changed" are the same event, so both
updates legitimately happen together. What genuinely never touches these fields is a live-path
assignment to an *already-existing* session — that data hasn't reached the saved list yet.

**Status:** resolved.

### B — No stated ordering constraint between `idle_eviction_seconds` and `finalize_unchanged_threshold_seconds`

**Problem:** both thresholds are checked against the same `last_updated_at` field, by two
different phases of the same cycle (§5.1 decides whether to flush live remnants to saved, §5.2
decides whether to finalize a saved session) — but nothing stated that
`finalize_unchanged_threshold_seconds` must be `>= idle_eviction_seconds`. If set smaller, a
session could become finalize-eligible under Phase 2's lower bar while still not stale enough to
have been flushed by Phase 1's higher bar — Phase 2 could finalize it off an incomplete range,
silently reintroducing the exact premature-finalization bug the two-phase design (§5) exists to
prevent. Both parameters were presented as independent tuning knobs with no stated relationship,
so nothing would have stopped this misconfiguration.

**Resolution:** the constraint is now stated explicitly in §3 (parameter table) and cross-referenced
in §5.2, with a requirement that config loading validate it and refuse to start otherwise (§7.1) —
not left as an implicit assumption two independently-tunable parameters could violate.

**Status:** resolved.

---

---

## Third pass — mixed-request routing & backward-walk redesign

A substantial redesign of §4.1/§4.3, prompted by a real gap in the original routing model: a
single incoming request has no guarantee all its images fall on the same side of `start_dct`, and
the original design (route the whole request as one unit, internal-split only by the arriving
group's own continuity) couldn't correctly handle an arriving batch that straddles an existing
session's boundary. Resolved across several rounds, all applied to
[`dis_sliding_window_split_merge_design.md`](dis_sliding_window_split_merge_design.md).

### D — Mixed-request routing: Group L / Group H split, with a mandatory processing order

**Problem:** §4.1 originally routed a whole request as one unit against `start_dct`. A request can
legitimately contain images on both sides.

**Resolution:** partition every image individually into Group L (`dct >= start_dct`, live path)
and Group H (`dct < start_dct`, historical path). **Group L must be processed first, in full,
before Group H begins** — not incidental ordering, but required: Group L's own processing can pop
pre-existing live entries purely from buffer pressure, extending the most recent saved session's
boundary in a way Group H's walk depends on seeing. Group H's own membership stays valid regardless
of how far Group L subsequently advances `start_dct`, since that only ever moves forward. Applied
in §4.1, with a concrete worked example of the ordering dependency.

**Status:** resolved.

### E — Historical processing redesigned around a backward walk instead of exhaustive search + internal-split

**Problem:** the original §4.3 (internal-split the arriving group first, by its own continuity;
then exhaustively check the whole group against every saved session) couldn't correctly handle an
internally-continuous arriving batch that straddles an existing session's boundary — nothing in
that design ever looked at *existing* session boundaries to decide how to split the arriving group,
only at the group's own internal structure.

**Resolution:** §4.3 walks the device's saved sessions backward (`dis_session_by_end`, §2.6) in
lockstep with Group H (also sorted descending) — a single merge-style pass, first proposed as a
two-pointer "bucket" classification (interior of a session → breaking; gap between two sessions →
extension/bridging/no-match), with §2.6 simplified from two range indexes down to one in the
process (sessions never overlap, so one ordering suffices — the walk no longer needs independent
range queries per direction).

**Status:** resolved (superseded by finding F below, which replaced the bucket mechanism itself
with a more precise one — the resolution *categories* here still stand).

### F — Interior-bucket asymmetry: the bucket approach couldn't detect multiple independent breaks within one session

**Problem:** gap-bucket resolution explicitly ran an internal-split check first (to catch two
mutually-discontinuous arriving clusters landing in the same inter-session gap); interior-bucket
resolution didn't — it assumed every image classified as "interior of `S_i`" belonged to one break
point. Concretely: `Sn = [10:00, 10:08, 10:16, 10:24]` (all legitimate 8-minute gaps), an arriving
batch of `[10:04, 10:20]` lands in *two different* gaps of `Sn` — genuinely two independent breaks
— but both were classified into the same "interior" bucket, and the bucket resolution had no way
to tell them apart. A first proposed fix (reuse the gap-bucket's internal-split check, i.e. mutual
continuity of the *arriving* images with each other) was shown to be insufficient: two arrivals can
be mutually close to each other while still needing separate breaks, if a confirmed existing member
of `Sn` sits between them (e.g. `10:04`/`10:12` around `Sn`'s own `10:08`) — that check only ever
looks at the arriving images' relationship to each other, never at what already exists between
them.

**Resolution — replaces the bucket mechanism entirely (supersedes E's bucket description):** a
direct backward **merge** of Group H against each visited session's own full dct list (not just its
cached range), descending order on both sides, grouping consecutive same-source picks and starting
a new group on every source switch. This directly discovers every position where a session's own
continuity is actually interrupted, rather than inferring it from the arriving group's structure.
Resolution then follows from what brackets each resulting group: session-sourced groups keep the
original id only if they're the oldest (chronologically last-consumed) one, everything else gets a
fresh id; arrival-sourced groups resolve as breaking (bracketed by the same session on both sides),
extension/bridging/no-match (bracketed by two different sessions), or one-sided gap logic (bracketed
on only one side, the leading/trailing edge). Tie-break on an exact dct match: the existing
session-sourced element is always consumed first, the arriving element treated as not-greater.

**Cost tradeoff accepted knowingly:** this reads each touched session's entire saved dct list, not
just its cached range summary — real work, not `O(1)` per session — so the walk's cost now scales
with total saved members touched, not just session count. Already reflected in §4.3's
lock-duration-tradeoff paragraph.

**Status:** resolved.

### G — `dis_embed_list`'s Redis type was flagged but never actually pinned down in the plan

**Problem:** `redis_reference.md` recommended a Sorted Set over Redis's native `LIST` (no built-in
sorted-insert) for `dis_embed_list`, but that finding never made it back into the design doc itself
— §2.1 still just said "a list, sorted by dct," and §8 didn't track it as an open item.

**Resolution:** §2.1 now pins `dis_embed_list` as a Redis Sorted Set (score = dct, member =
serialized `(session_id, embedding)`), with the concrete consequence spelled out — since
`session_id` lives inside the member string, relabeling an entry during a merge or split (§4.2) is
a `ZREM`+`ZADD` pair, not an in-place field update. `redis_reference.md` updated to match (no
longer hedges on "if implemented as a ZSET").

**Status:** resolved.

### H — §7.2's method-reference range was stale after the routing redesign

**Problem:** §7.2 said Recognize runs "the full insert/resolve step (§4.1-§4.2)" — but after finding
D, §4.1 can route part of a request to §4.3 (historical) too; §7.2 only referenced §4.2.

**Resolution:** §7.2 now says "§4.1" (the routing/resolve step as a whole) and explicitly notes a
single request may invoke both §4.2 and §4.3.

**Status:** resolved.

### I — The merge walk (finding F) didn't separate arrival elements landing in open space between sessions

**Problem:** found on a dedicated adversarial re-read of the merge walk specifically, after
believing it was settled. The merge walk correctly separates arrival elements when session data
sits between them (that's what fixed finding F's `10:04`/`10:20` case), but a group only ever
switches when a *session*-sourced element interrupts an arrival run — in the open space *between*
two sessions (or past the leading/trailing edge), there's no session data to do that, so several
mutually-unrelated arrivals can get merged into one group purely because nothing existed to
interrupt them. Concrete example: `Sn = [10:00]`, next-older `S_{i-1} = [08:00]`, arrivals
`[09:52, 09:20, 08:05]` — all three consumed as one group by the walk, but `09:52` should
backward-extend `Sn`, `08:05` should forward-extend `S_{i-1}`, and `09:20` belongs to neither —
resolving them as one aggregate group would silently drag all three into whichever side the
group's overall range happened to favor.

**Resolution:** reinstates mutual-continuity internal-split, but scoped precisely — **only** for
arrival-sourced groups classified as gap or edge type (bracketed by two different sessions, or open
on one side), never for interior-classified groups (where the merge walk's own separation via
session data is already sufficient and provably correct, per finding F). Interior groups still
resolve as one unconditional break; gap/edge groups now split by mutual continuity first, then each
resulting sub-group resolves independently against its bracketing session(s). This is *not* a
reversion to the originally-rejected "internal-split by arrival continuity" idea from finding F —
that was wrong specifically for interior regions (it ignored existing session data sitting between
arrivals); it's correct here because no such data exists in the gap/edge regions to ignore in the
first place.

**Status:** resolved.

---

This file is ready to receive findings from the next review pass.
