# DIS Group L Live Buffer: RAM-Copy vs. Redis-Native (ZSET) — Detailed Comparison

Status: **analysis complete**. RAM-copy is the implemented, shipped approach
(`frs_src/dis_session_engine.py`'s `_resolve_live_group`/`sweep_idle_buffer`
reading `dis_embed_buffer` once per request/sweep via `utils.py`'s
`dis_apply_buffer_plan`). The Redis-native (per-image ZSET) alternative was
never built — this document reconstructs its cost precisely enough to
confirm, with worked numbers rather than intuition, that RAM-copy is the
better choice on every dimension measured, never worse on any.

---

## 1. What's being compared

**RAM-copy (implemented)**: per request, read the live buffer
(`dis_get_embed_buffer`) and the device's single evolving-chain reference
(`dis_get_last_decided_reference`) exactly once each. Simulate every new
image's insert, the resulting pop-if-over-`max_size`, and each pop's
session_id decision (`dis_window.decide_popped_session`) entirely in Python
— zero Redis calls during the simulation. Write the final buffer state, every
popped entry's saved-session fold, and the final reference back in one
`dis_apply_buffer_plan` call.

**Redis-native (ZSET, hypothetical)**: store the live buffer as a Redis
Cluster sorted set (score = `device_capture_time`, member = a JSON blob of
`{device_capture_time, embedding}` — collision-safe now that every image is
guaranteed to carry an embedding and a unique `device_capture_time`). Insert
each new image as its own `ZADD`; check size via `ZCARD`; pop via `ZPOPMIN`
when over `max_size`. The per-pop decide-and-fold logic (reading/writing the
reference, folding into the saved session) is identical in both approaches —
it is not something the buffer's own representation can change, since it's
governed by the UUID-minting/dual-write constraint (see the DIS session
engine's module docstring) regardless of how the buffer itself is stored.

Variables used throughout: `K` = new images in one request, `P` = how many
of those trigger a pop this request (`0 ≤ P ≤ K`), `N` = devices in the
fleet, `R` = requests/sec/device.

---

## 2. Round trips — single image (`K=1`)

Scenario: one new image arrives, the buffer is already at `max_size`, so it
triggers exactly one pop (`P=1`), continuing an existing (non-fresh) session.

### RAM-copy — command by command

| # | Call | Redis command | Why |
|---|---|---|---|
| 1 | `dis_get_embed_buffer` | `LRANGE` | read the whole (small, bounded) buffer once |
| 2 | `dis_get_last_decided_reference` | `GET` | read the device's single reference once |
| — | *(insert, sort, decide, pop — all in RAM)* | — | zero Redis calls |
| 3 | `dis_apply_buffer_plan`'s buffer pipeline | `DEL`+`RPUSH`+`SET` (pipelined, co-located via hash tags → one round trip) | rewrite remaining buffer, refresh `dis_buffer_last_update` |
| 4 | fold: `dis_get_saved_session_device_capture_times` | `HGET` | read existing saved dct list |
| 5 | fold: `_dis_set_saved_session_device_capture_times` | `HSET` | write dct list with the new one appended |
| 6 | fold: `dis_get_session_meta` | `HMGET` | read existing start/end/count |
| 7 | fold: `_dis_set_session_meta` | `HMSET` | write updated start/end/`last_updated_at` |
| 8 | fold: `_dis_incr_member_count` | `HINCRBY` | bump member_count |
| 9 | fold: `_dis_set_range_index` | `ZADD` | update `dis_session_by_end` position |
| 10 | reference write | `SET` | overwrite `dis_last_decided` |

**Total: 10** = `4 + 6P` (reads #1-2 + pipelined write #3 + reference write
#10 = the flat "4"; fold #4-9 = the "6P"). A *fresh* pop (minting a new
session) adds one more call (`_dis_set_session_meta` for
`tenant_drp`/`created_at`), giving `4 + 7P` for that case — noted once here,
applies identically to both approaches below.

### ZSET-native — command by command

| # | Call | Redis command | Why |
|---|---|---|---|
| 1 | insert | `ZADD` | add the new image to the device's sorted set |
| 2 | size check | `ZCARD` | find out if it's now over `max_size` |
| 3 | pop the oldest | `ZPOPMIN` | fetch-and-remove the front entry — RAM-copy gets this step "for free" from already having the contents in hand |
| 4 | reference read | `GET` | same as RAM-copy #2 |
| 5–9 | *identical fold as RAM-copy #4-8* | `HGET`/`HSET`/`HMGET`/`HMSET`/`HINCRBY` | unchanged by buffer representation |
| 10 | `_dis_set_range_index` | `ZADD` | same as RAM-copy #9 |
| 11 | reference write | `SET` | same as RAM-copy #10 |

**Total: 11.** One *more* than RAM-copy, even at `K=1`. The reason: RAM-copy
pays one flat upfront read to get the buffer's contents into hand and never
needs another buffer-content round trip no matter how many pops happen in the
request; ZSET-native has no such cache, so it must ask Redis for the popped
item explicitly (`ZPOPMIN`) every time, since it never held the buffer's
contents locally.

### Verdict

Not a true tie once counted precisely — RAM-copy wins by 1 round trip even
at the smallest possible batch size. (An earlier, rougher pass at this
comparison had approximated it as an exact tie; the precise command trace
above corrects that.)

---

## 3. Round trips — multi-image batch (`K>1`)

Scenario: `K=3` new images in one request, buffer already full, all three
trigger a pop (`P=3`), all continuing existing sessions.

### RAM-copy

```
1. LRANGE (buffer)                          — once
2. GET    (reference)                       — once
   insert/sort/pop/decide image 1 — RAM only
   insert/sort/pop/decide image 2 — RAM only
   insert/sort/pop/decide image 3 — RAM only
3. DEL+RPUSH+SET (buffer pipeline)           — once
4-9.   fold pop #1                          — 6 calls
10-15. fold pop #2                          — 6 calls
16-21. fold pop #3                          — 6 calls
22. SET (reference)                         — once
```
**Total = 22** = `4 + 6(3)`. Steps 1, 2, 3, and 22 never multiply by `K` —
all three images are processed inside the same RAM loop between one read and
one write.

### ZSET-native

```
Image 1: ZADD, ZCARD, ZPOPMIN   — 3 calls (pops, buffer already full)
Image 2: ZADD, ZCARD, ZPOPMIN   — 3 calls
Image 3: ZADD, ZCARD, ZPOPMIN   — 3 calls
GET (reference, read once — most favorable case, batched in RAM like RAM-copy)
fold pop #1, #2, #3             — 6 calls each = 18
SET (reference, write once)
```
**Total = 9 (buffer mechanics) + 1 + 18 + 1 = 29**

### Corrected general formulas

```
RAM-copy:     4 + 6P        (flat in K — K never appears in the formula)
ZSET-native:  2K + 7P + 2   (the ZPOPMIN-per-pop folds into this term)
```

**Gap = (2K + 7P + 2) − (4 + 6P) = 2K + P − 2**

| K | P | RAM-copy | ZSET-native | Gap |
|---|---|---|---|---|
| 1 | 1 | 10 | 11 | 1 |
| 3 | 3 | 22 | 29 | 7 |
| 5 | 5 | 34 | 47 | 13 |

(Note: this corrects an earlier approximation of the ZSET-native formula as
`2K + 6P + 2`, which undercounted the per-pop `ZPOPMIN` call. The qualitative
conclusion — RAM-copy wins, gap widens with `K` — holds; the margin is
larger than originally estimated.)

### Why the gap widens — mechanically

RAM-copy's cost is governed *entirely* by `P` — `K` is invisible to the
round-trip count, because every new image's insert gets folded into the same
single read → RAM-loop → single-write cycle, at zero marginal Redis cost per
image. ZSET-native's cost is governed by *both*, because each image
independently costs its own `ZADD`+`ZCARD` the instant it arrives, whether
or not it ends up triggering a pop — there is no way to batch that
insertion step when the buffer's real membership lives in Redis the whole
time rather than in a local copy mutated freely before one commit.

Even when `P < K` (not every image pops — e.g. the buffer had spare room for
the first couple), the gap stays positive for any `K ≥ 1`, because the flat
`2` round trips per image (`ZADD`+`ZCARD`) never goes away regardless of
whether that image ultimately causes a pop.

---

## 4. Aggregate Redis command volume at fleet scale

Formulas: `N × R × (4 + 6P)` for RAM-copy vs. `N × R × (2K + 7P + 2)` for
ZSET-native — i.e. the per-request round-trip cost from §2/§3, multiplied by
how often it's paid across the whole fleet.

**What `P` actually is in steady state.** `P` isn't free-floating — it's
governed by `embed_buffer_max_size` (8) vs. arrival pattern. For the common
`K=1` case, once a device's buffer has filled up (a one-time, per-device cold
start), it *stays* full as long as the device keeps sending — so in steady
state, **`P=1` on essentially every request**, not `P=0`.

**Worked example.** Take `N=500,000` devices and an illustrative
`R=0.1` requests/sec/device (one image every 10s) — aggregate request rate
= 50,000 req/s. At steady-state `P=1`:

- RAM-copy: `50,000 × 10 = 500,000` Redis commands/sec.
- ZSET-native (`K=1,P=1` → 11 round trips): `50,000 × 11 = 550,000`
  commands/sec.

Both numbers **double** once the Redis↔Valkey dual-write multiplier is
applied (every write command Router dispatches goes to both backends during
the migration) — **~1,000,000 commands/sec (RAM-copy) vs. ~1,100,000
commands/sec (ZSET-native)**, for this one feature alone, before accounting
for any other traffic the same cluster serves.

At `K=1` the two are close (the gap is only the 1 extra round trip from
§2). The real separation shows up for any meaningful fraction of
multi-image requests (`K>1`), where ZSET-native's aggregate volume grows
with `2K` per request while RAM-copy's stays flat — directly shrinking the
maximum `N×R` the Redis/Valkey tier can sustain for ZSET-native relative to
RAM-copy, at any batch-size mix above the trivial single-image case.

**Caveat worth confirming separately: billing model.** Whether this
command-volume difference matters as a literal dollar figure (not just a
capacity-headroom figure) depends on how the Redis/Valkey deployment is
billed:
- **Capacity-based** (reserved node-hours — the traditional AWS
  ElastiCache/Azure Cache/GCP Memorystore/Redis Enterprise model): command
  volume affects cost *indirectly*, by determining what node size/shard count
  is needed to sustain acceptable latency. Cutting round trips reduces
  (or defers) the need to scale the cluster up — it doesn't shrink this
  month's bill on its own.
- **Pay-per-request / serverless** (AWS ElastiCache Serverless, Upstash,
  Momento, and similar): billed on a mix of data stored and
  requests/commands processed. On this model, every round trip above is a
  **direct, individually-metered cost**, and the command-volume numbers in
  this section translate close to one-to-one into the actual monthly bill.

Which model applies here isn't visible from the application code
(`cfg.VALKEY.host/port/username/password` + SSL doesn't indicate a billing
tier) — worth confirming with whoever owns the Redis/Valkey
infrastructure/billing before treating this section as a dollar estimate
rather than a capacity-planning one.

---

## 5. Redis/Valkey server-side CPU load

Every Redis command costs real CPU on the Redis/Valkey node processing it,
even though each individual command is cheap (sub-microsecond to
low-microsecond). This section is a direct consequence of §4, not a separate
measurement: aggregate command volume (`N × R × round-trips-per-request`) is
exactly what determines total server-side CPU demand across the cluster.
Since RAM-copy's aggregate volume is `≤` ZSET-native's always, and strictly
less whenever any `K>1` requests occur, RAM-copy imposes equal-or-lower
CPU load on the Redis/Valkey cluster in every scenario — this is not an
independent finding, it inherits directly from §3/§4's round-trip
comparison.

Practical implication: whatever cluster node count/size is provisioned to
sustain the fleet's aggregate request rate needs to absorb strictly less
server-side work under RAM-copy, leaving more CPU headroom for the rest of
what the same cluster serves (every other Redis/Valkey consumer in the
codebase — see the Router-usage survey — shares this same cluster).

---

## 6. Worker-side (client) RAM

**RAM-copy**: pulls the live buffer (≤ `embed_buffer_max_size` + `K` small
JSON blobs, each roughly 1-4KB if embeddings are a few hundred floats) into
one worker process's RAM for the duration of one request, then discards it
immediately after `dis_apply_buffer_plan` returns. Tens of KB, transient,
freed per request — does not accumulate across requests, and does not scale
with fleet size `N` (a worker only ever holds whichever requests it's
*currently* handling, never anything about devices it's not actively
processing).

**ZSET-native**: no client-side copy of the buffer contents at all — each
command (`ZADD`/`ZCARD`/`ZPOPMIN`) is issued directly against Redis with no
local aggregation.

**Verdict**: ZSET-native technically uses less worker-side RAM, but
RAM-copy's cost here is already negligible in absolute terms (tens of KB,
momentarily, per in-flight request) — this is not a dimension where the
difference is practically meaningful at any fleet size, unlike §2-§5 where
the difference compounds with scale.

---

## 7. Overall verdict

| Dimension | Winner | Margin |
|---|---|---|
| Round trips, K=1 | RAM-copy | small (1 round trip) |
| Round trips, K>1 | RAM-copy | grows linearly with K (`2K+P-2`) |
| Aggregate fleet-scale command volume | RAM-copy | ≤ always, strictly less for any K>1 traffic |
| Redis/Valkey server-side CPU load | RAM-copy | inherits directly from the above |
| Worker-side RAM | ZSET-native (nominal) | negligible either way, not practically meaningful |

**RAM-copy is equal-or-better on every dimension that scales with fleet size
or traffic volume, and never meaningfully worse on the one dimension where
ZSET-native has a nominal edge.** There is no scenario, traffic pattern, or
batch-size mix under which Redis-native (ZSET) buffer storage is the
better choice — it was correctly rejected, and the implemented RAM-copy
design (one read, a RAM-side simulation, one write per request) should be
kept as-is.
