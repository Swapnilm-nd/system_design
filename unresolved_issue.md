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

This file is ready to receive findings from the next review pass.
