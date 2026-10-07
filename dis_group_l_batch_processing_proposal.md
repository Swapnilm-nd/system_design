# DIS Group L Batch Processing — Alternative Live-Path Algorithm (Proposal)

Status: **proposal, not yet adopted**. This document fully specifies an alternative to
§4.2 of `dis_sliding_window_split_merge_design.md` — processing a request's Group L
images as one batch (read once, decide all, apply once) instead of one image at a
time (read-decide-apply per image). It was developed to reduce per-image Redis round
trips and lock acquisitions for multi-image Group L batches. Every edge case below was
found by deliberately trying to break an earlier version of the rule with worked
examples; each is stated with the example that exposed it.

**This is a real behavioral change, not just a performance optimization** — see
"Divergence from the current algorithm" below. It needs explicit sign-off before
implementation, not just a performance justification, because it contradicts
§4.1's literal "never as a batch, even within one multi-image request" and because
one of the open findings below (saved-session continuation) is arguably a gap in the
*current, already-shipped* algorithm too, independent of whether batching is adopted.

---

## 1. Shape of the algorithm

Per request, for the images routed to Group L (§4.1's routing is unchanged):

1. **Insert** every Group L image into the live buffer, unassigned (`session_id = None`),
   sorted by `device_capture_time` alongside the existing entries. No popping yet — the
   buffer is temporarily allowed to exceed `embed_buffer_max_size`.
2. **Decide** every unassigned entry's `session_id`, walking the buffer once, in order,
   via the state machine in §2.
3. **Pop** from the front until the buffer is back to `embed_buffer_max_size`, flushing
   each popped entry to saved storage exactly as `pop_and_save` does today — using the
   *final* resolved `session_id`, not whatever it had mid-walk.

All three steps happen under **one** per-device lock acquisition (§5).

---

## 2. The decision walk

One pass, left to right over the assembled (unsorted-session, sorted-dct) buffer.
State: `last_seen_session_id`, initialized to `None` once per request-level walk (not
reused across requests).

### 2.1 `check(a, b)` — the comparison primitive

Not one uniform function — it has two modes:

- **Live vs. live** (both sides are real images with embeddings): standard rule —
  passes iff `cosine_similarity(a.embedding, b.embedding) > similarity_threshold`
  **and** `abs(a.dct - b.dct) < dct_gap_threshold_seconds`.
- **Live vs. saved-session stand-in** (one side has no embedding — see §2.4): **gap-only**
  — passes iff `abs(a.dct - b.dct) < dct_gap_threshold_seconds`. No similarity check is
  possible, since saved (already-popped) session data carries no embedding (§2.2 of the
  base design).
- **Either side absent** (no `prev`, or no `next`): always fails. Mirrors the base
  design's convention that a missing neighbor never passes (§1).

### 2.2 Per-row logic

```
for each row, left to right:
    if row.session_id is None:                      # unassigned
        match = check(prev, row)                    # prev may not exist -> match=False
        if match and last_seen_session_id is None:
            row.session_id = prev.session_id
            last_seen_session_id = row.session_id
        elif not match and last_seen_session_id is None:
            row.session_id = new_session_id()
            last_seen_session_id = prev.session_id if prev exists else None
        elif match and last_seen_session_id is not None:
            row.session_id = prev.session_id
        else:  # not match and last_seen_session_id is not None
            row.session_id = new_session_id()

    else:                                             # already assigned
        if last_seen_session_id is None:
            pass                                      # nothing to reconcile
        else:
            match = check(prev, row)
            if match and row.session_id == prev.session_id:
                pass
            elif not match and row.session_id == prev.session_id:
                relabel row and every contiguous row ahead sharing row.session_id -> new_session_id()
            elif match and row.session_id != prev.session_id:
                # MERGE - see §2.3, not a fresh id
                relabel row and every contiguous row ahead sharing row.session_id -> prev.session_id
                reconcile_saved_data(abandoned=row.session_id, surviving=prev.session_id)  # §4
            else:  # not match and row.session_id != prev.session_id
                if row.session_id == last_seen_session_id:
                    # THREE-WAY SPLIT - prev and row were one session before this walk
                    relabel row and every contiguous row ahead sharing row.session_id -> new_session_id()
                # else: pass - prev and row were never the same session, nothing to do
            last_seen_session_id = None   # consumed - one-shot, reset immediately after use
```

### 2.3 Merge vs. split — the distinction that must not collapse (closes Gap 1)

The `match and row.session_id != prev.session_id` branch is a **merge**: `prev`'s id
already represents an established session (it either came from the live buffer
unchanged, or was itself just assigned from *its own* prev). The correct action is to
relabel `row`'s run to **adopt `prev`'s existing id**, not mint a third, unrelated id —
and to run saved-data reconciliation (§4), because the abandoned id may already have
history in `dis_saved_sessions` that must not be orphaned.

**Worked example (the bug this closes):** buffer `[100(S1,A), 200(S1,A), new(300,A),
700(S2,A)]`. New image at 300 matches both neighbors (prev=S1 passes, next=S2 passes,
different ids) → genuine merge. A rule that just says "ids differ → mint new id for the
far side" would turn one real session into two disconnected ones and — if `S2` already
had older saved data under its own name — silently strand it under a dead id forever.

### 2.4 Three-way split detection (closes Gap 2)

The `not match and row.session_id != prev.session_id` branch needs to know whether
`prev` and `row` **were already the same session before this walk even started** — that's
exactly what `last_seen_session_id` answers. If yes, inserting the new row broke an
assumed continuity, and `row`'s run must peel off into its own id even though neither
side matched the new entry. If no, nothing needs to change.

**Worked example:** buffer `[500(S1,A), new(800,C), 1050(S1,A)]`. The new image at 800
matches neither neighbor (different driver). `prev` and `next` were `S1` both before
this insert → three-way split: `800` isolates, `1050` onward peels to a *different*
fresh id, `500`'s side keeps `S1` untouched. Without the `last_seen_session_id` check,
a naive "ids differ → leave as-is" rule would leave `S1` nominally spanning `500..1050`
with an unrelated session's image physically sitting inside its range — breaking the
"a session's own `[start,end]` never wrongly includes another session's members"
guarantee (§4.4 of the base design) that `sessions_eviction`'s DB range-query depends on.

### 2.5 No `prev` at all (closes Gap 4)

Two sub-cases, both needing an explicit rule (a naive implementation would crash or do
something undefined, not just something subtly wrong):

- **`check(None, row)` must be defined as `False`**, not left to whatever happens when
  code tries to read a nonexistent object's fields.
- **`last_seen_session_id = prev.session_id if prev exists else None`** — when there's
  truly nothing before this row (buffer start), there's no prior session to remember,
  so it stays `None`, not an attempt to read a field off nothing.

This is not an exotic corner case — it's hit by every device's very first-ever request
(empty buffer, `start_dct = None`, everything routes to Group L), and can also be hit
by a `dct` tie at the Group L/Group H routing boundary (`dct >= start_dct` is inclusive).

### 2.6 Buffer-empty but saved sessions exist (Gap 4 extension)

`start_dct == None` does **not** mean there's no established session for this device —
a session can sit in `dis_saved_sessions`, not yet finalized by `sessions_eviction`
(which only fires once a session's `last_updated_at` crosses the staleness bar, up to
an hour later). A device that goes idle, drains its live buffer, and comes back online
within that window should be able to continue its prior session, not get wrongly
isolated into a throwaway one-image session.

**Rule:** when `start_dct == None` and the device has saved sessions, before inserting
the new Group L images, prepend one synthetic stand-in row representing the
most-recently-ended saved session (`dis_session_by_end`'s highest-scoring entry):
`{dct: that session's end_dct, session_id: that session's id, embedding: None}`. Then
run the *exact same* walk in §2.2 over `[stand-in, ...new images]` — no separate logic
needed. The stand-in's lack of an embedding is exactly what makes `check()`'s
gap-only mode (§2.1) apply to comparisons against it, and nothing else.

**Why no separate sub-grouping step is needed:** the walk's own chain of consecutive
`prev`↔`row` checks *is* the sub-grouping — each new image is compared to whatever
immediately precedes it (the stand-in, or an already-decided earlier image in the same
batch), exactly like the base design's historical-path `split_into_subgroups` does
explicitly. Tracing `[stand-in(dct=1000, S_prev), Q1(1200,A), Q2(1900,C), Q3(1950,C)]`:
Q1 matches the stand-in (gap 200, same family as the *saved* session... though only gap
is actually checked) → adopts `S_prev`. Q2 fails against Q1 (gap 700) → fresh id `S_new`.
Q3 matches Q2 → joins `S_new`. Correct result without any extra pass: `Q1 → S_prev`
(extended), `Q2, Q3 → S_new`.

**Known risk, flagged but not fixed here:** the current, already-shipped sequential
algorithm (`resolve_live_matrix`) never consults saved sessions at all when there's no
live `prev` — it always mints a fresh id. This proposal's fix for Gap 4 is broader than
"make batching work"; it's arguably a correctness gap in production today, independent
of whether this batching proposal is ever adopted. Worth raising separately.

### 2.7 No `next` at all (buffer tail)

Symmetric to §2.5 — treat "no next exists" the same as "next fails" (not the same as
"next's session_id is None"). Only `prev` is considered for the last row in the buffer.

---

## 3. Chained merges within one batch (closes Gap 3)

A single batch can trigger more than one merge, where the second merge's surviving
session is the *abandoned* side of the first. Naively re-reading Redis fresh for each
merge's reconciliation check silently loses data, because earlier merges in the same
batch haven't been written to Redis yet (step 3 of §1 — all writes are deferred to the
very end).

**Worked example:** pre-existing saved data `S_X: [50]`, `S_Y: [300]`, `S_Z: [2000]`.
Batch triggers merge 1 (`S_Y` → `S_X`) then merge 2 (`S_X`, now including `S_Y`'s
folded-in data → `S_Z`... or into `S_X` surviving again, direction doesn't matter for
the point). If merge 2's reconciliation re-reads `S_X` straight from Redis, it sees only
the original `[50]` — merge 1's fold-in of `[300]` was never written there — and the
final combined value silently drops `S_Y`'s `[300]` forever.

**Fix:** maintain an in-memory map, `saved_data_overlay`, keyed by `session_id`, holding
each touched session's current (saved dct list, start, end, member_count) as updated by
every merge decided so far in this batch.

- **Lookup** ("what is session `Q`'s current saved data/meta?"): check `saved_data_overlay`
  first; only fall back to a real Redis read if `Q` hasn't been touched yet this batch —
  and seed the overlay with that read result immediately, so later lookups in the same
  batch hit the cache instead of re-reading (now-stale) Redis state again.
- **Write** (whenever a merge computes a combined value): write the surviving session's
  new combined state into `saved_data_overlay[surviving_id]`. Record the abandoned id in
  an `abandoned_ids` set and drop it from the overlay if present.
- **Final flush** (the one write-to-Redis step at the end of the batch): for every id in
  `saved_data_overlay`, write its final combined dct list/meta to
  `dis_saved_sessions`/`dis_session_meta`/`dis_session_by_end`. For every id in
  `abandoned_ids`, delete its bookkeeping entirely.

Re-run with the fix: merge 1 → `overlay[S_X] = [50, 300]`, `abandoned_ids = {S_Y}`.
Merge 2 looks up `S_X` → hits the overlay → correctly combines `[50, 300]` with `S_Z`'s
`[2000]` → `overlay[S_X] = [50, 300, 2000]`. Final write: `S_X → [50, 300, 2000]`;
`S_Y`, `S_Z` deleted. Nothing lost.

---

## 4. Lock scope

One lock acquisition covers the **entire** Group L batch for a request — the initial
read, the whole decision walk (§2), and the single final apply/pop step (§1 step 3).
Not per-image.

This is consistent with, not a new kind of exception to, the base design's existing
lock-scope rule: the historical path already holds one lock across its whole
(potentially multi-session) backward walk, and `sessions_eviction` already holds one
lock across both of its phases together. Group L's batch joining that pattern is the
same principle applied to a third case, not a special one.

**Group L and Group H remain two separate acquisitions**, not one spanning the whole
request — there's no benefit to holding the lock through Group H's (possibly unbounded)
backward walk just because Group L happened to run first; that only extends an
already-long critical section for no reason.

---

## 5. Group H ordering invariant — confirmed to still hold

§4.1 of the base design requires Group L to fully complete (including its pops) before
Group H's backward walk begins, because Group L's pops can extend a saved session's
boundary that Group H needs to already see. Under this batched model, Group L is still
one complete, atomic unit (lock → read → decide → apply, including every deferred pop)
that fully finishes and releases its lock before Group H's own lock/read/decide/apply
sequence starts. This holds **by construction** — nothing new needs to be built to
preserve it; batching only changes what happens inside Group L's own turn, not when
Group H's turn starts relative to it.

---

## 6. Divergence from the current algorithm — accept knowingly, not by accident

This is not purely a performance change. A concrete case where the two algorithms
produce genuinely different session groupings for identical input:

Buffer `[100(S_A, F1), 700(S_B, F2)..1000(S_B, F2)]` (`S_A`/`S_B` split by a `dct` tie).
Group L = `[Y: dct=300, F1 (matches S_A)]`, `[X: dct=650, F2 (matches S_B)]`.

- **Sequential (current code):** inserting `Y` pops `S_A`'s only live entry *before* `Y`
  is decided → `Y` sees no `prev` at all → isolated into a brand-new session. Inserting
  `X` next pops `Y`'s own entry → `X` also sees no `prev` → joins `S_B`. Net: `S_A`
  unchanged, a throwaway one-image session created for `Y`, `X` joins `S_B`.
- **Batched (this proposal):** `Y` is decided before anything pops, so it correctly
  sees `S_A` as `prev` and matches it → `Y` extends `S_A`. `X` then fails against `Y`
  (now `S_A`, different family) and joins `S_B` on its own merits. Net: `S_A` grows by
  one member, no throwaway session.

The batched answer is arguably *more* correct — the sequential algorithm's answer here is
an artifact of which image in a multi-image request happened to be decided first and
which pop that triggered, not a signal about the actual driver. But it is a genuine,
measurable behavioral difference from what's shipped and from the base design doc's
explicit "never as a batch" instruction — this needs sign-off from whoever owns that
doc, not a quiet swap.

---

## 7. Summary of trade-offs

| | Current (sequential, per-image) | This proposal (batched) |
|---|---|---|
| Redis round trips per Group L image | ~4 (lock, read, apply, release) | ~4 total for the *whole batch*, not per image |
| Lock hold time | Many short holds | One longer hold per batch |
| Order-dependence artifacts (e.g. §6's example) | Present, unaddressed | Removed for images within one batch |
| Matches base design doc's literal wording | Yes | No — needs explicit sign-off |
| Implementation complexity | Lower (clean one-image-in/one-plan-out unit) | Higher (shared walk state, overlay map, virtual stand-in rows) |
| Saved-session continuation on buffer-empty (§2.6) | Not handled (gap in current prod code too) | Handled, but applies regardless of whether batching itself is adopted |

## 8. Open items not covered by this document

- Whether §2.6's saved-session-continuation fix should be back-ported to the current
  sequential algorithm *regardless* of whether this batching proposal is adopted.
- Whether `sessions_eviction`'s own future interaction with an in-flight batched write
  needs any additional consideration (not specifically re-examined here beyond the lock
  scope in §4, which is unchanged from the base design's existing rule).
