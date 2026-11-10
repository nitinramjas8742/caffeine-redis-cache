# Hybrid Cache — End-to-End Flow (Read & Update)

> **The one rule:** a pod may serve a cached entity only if it cached it **after**
> the last commit that touched it.
>
> Everything else is mechanics around that sentence:
>
> - **Local timestamp map** — "when did *this pod* cache this key?"
> - **Global timestamp map (Redis)** — "when did *any pod* last commit a change to this key?"
> - On every read: `if global > local → throw the payload away and reload from the database`.
> - If the global timestamp cannot be read at all, the pod **fails closed** and reads
>   the database. It never serves a payload it cannot justify.
>
> The **SoftLock** is the exception to the rule: timestamps only move **on commit**,
> so during an in-flight update the old timestamp still looks valid. The SoftLock
> marks that window.

---

## Contents

- [Design history](#design-history)
- [Serving only current data](#serving-only-current-data)
- [Minimizing stale reads](#minimizing-stale-reads)
- [Race conditions and how they are avoided](#race-conditions-and-how-they-are-avoided)
- [End-to-end in six steps](#end-to-end-in-six-steps)

1. [The cast](#1-the-cast)
2. [Phase 1 — Build time](#phase-1--build-time)
3. [Phase 2 — Pod startup](#phase-2--pod-startup)
4. [Phase 3 — First read (cold)](#phase-3--first-read-of-an-entity-cold)
5. [Phase 4 — Second read (hot)](#phase-4--second-read-hot)
6. [Phase 5 — The SoftLock window](#phase-5--the-softlock-window)
7. [Phase 6 — Update on pod A](#phase-6--update-on-pod-a-the-full-write)
8. [Phase 7 — Pod B after the update](#phase-7--pod-b-after-the-update)
9. [Phase 8 — Delete / evict / clear](#phase-8--delete--evict--clear)
10. [Phase 9 — Natural-id and query cache](#phase-9--natural-id-and-query-cache)
11. [Phase 10 — Failure modes](#phase-10--failure-modes)
12. [Deployment topologies](#deployment-topologies-standalone--sentinel--cluster)
13. [Call-count summary](#call-count-summary)
14. [60-second summary](#the-60-second-summary)
15. [Assumptions and caveats](#assumptions-and-caveats)
16. [Running and testing](#running-and-testing)

---

## Design history

There were two designs. Only the second one is implemented; the first is kept
here because it explains why the current rules are strict.

### Design A — degraded serving (removed)

When Redis was unavailable, entity and collection reads served the local payload
from Caffeine. The node stayed "available" by serving possibly stale data, and a
reconnection strategy decided what happened when Redis returned:

| Strategy | Behaviour |
|---|---|
| `NONE` | keep the local mirror and payloads |
| `CLEAR` | drop them and re-read from Redis/database |
| `LOAD` | in this codebase, identical to `CLEAR` |

`NONE` was the default. That was the problem. A write that commits **during** the
outage updates the database but cannot `HSET` its new timestamp or `PUBLISH` an
invalidation. When Redis returns, its hash still holds the old timestamp. A pod
that cached the pre-outage payload then evaluates

```
global timestamp (old) <= local timestamp (old)  →  "valid"
```

and keeps serving the stale row. `CLEAR` happened to fix this on recovery; `NONE`
could leave the stale value in place indefinitely. It also served a payload during
the outage that the timestamp registry could not prove current.

### Design B — fail-closed (current)

The current design removes the strategy switch entirely:

| Situation | Current behaviour |
|---|---|
| Redis healthy | timestamp comparison + local near-cache; zero network on hot reads |
| Redis unavailable | **reads miss and go to the database**; local payloads are evicted |
| Cache fill while Redis is down | **not cached**; the local tier is not repopulated |
| Committed write while Redis is down | not cached locally; the DB remains authoritative |
| Redis or pub/sub recovery | **always** clear every local timestamp and payload |
| After the recovery clear | `near-cache.recovery-grace-ms` of forced DB revalidation |

The contract is: **serve a payload only when the timestamp registry proves it
current; otherwise read the database.** A Redis outage therefore costs database
latency, not correctness. Query timestamps already worked this way; the entity and
collection paths now match them.

The cost is deliberate: during an outage the database absorbs what the cache
normally absorbs. That is the trade for "no stale reads" and should be sized for
(connection pool, timeouts, and load shedding).

---

## Serving only current data

The decision is made in `AbstractTimestampAwareStorage.validateCacheEntryBasedOnTimeStamp`
and enforced by the surrounding write and recovery paths.

A payload is served only when **all** of the following hold:

```
1. no forced revalidation window is active
2. Redis is reachable (isRemoteAvailable() == true)
3. the global timestamp exists
4. the global value is not a SoftLock marker (or it is this pod's own lock)
5. a local timestamp exists for the payload
6. global timestamp <= local timestamp
```

Anything else is a miss. In particular:

| Condition | Result |
|---|---|
| `isRemoteAvailable() == false` | evict local payload, return miss → DB |
| global timestamp missing | evict local payload, return miss → DB |
| global is a peer's SoftLock | evict local payload, return miss → DB |
| local timestamp missing | evict local payload, return miss → DB |
| `global > local` | evict local payload, return miss → DB |
| `global <= local` | serve the Caffeine payload |

The other half of the guarantee is that unverifiable state is not created:

- **DB fill** (`storeFromLoad`): skipped while Redis is unavailable. When Redis is
  available it **adopts** the timestamp it read; it does not mint a new one, so a
  fill never looks like a write.
- **Commit** (`storeWrite` with the local lock): mints a new `System.currentTimeMillis()`
  timestamp, writes it with `HSET`, then publishes. If Redis is unavailable, the
  value is not cached locally.
- **Recovery**: `clearLocalStateOnRecovery()` always drops the mirror and payloads,
  so nothing cached during the outage survives into the healthy state.
- **Query cache**: `DomainQueryTimestampsRedisStorage` already returns a miss when
  Redis is unavailable or the timestamp is missing.

The net effect is simple to state: there is no code path that says "Redis is
unavailable, so serve the local copy."

---

## Minimizing stale reads

"Serving current data" is the hard rule. "Minimal stale reads" is about the healthy
path: how quickly a peer stops serving a payload after another pod commits.

The window is:

```
writer: HSET new timestamp  →  PUBLISH invalidation
peer:                              receive invalidation  →  evict mirror + payload
peer next read:                    HGET new timestamp  →  compare  →  DB reload
```

Until the peer processes the invalidation, it can still serve its local payload.
The extension keeps that window as small as it can:

| Mechanism | What it bounds |
|---|---|
| Redis pub/sub `INVALIDATE` | the normal window: one message hop after the commit |
| `HSET` before `PUBLISH` | the timestamp is already authoritative when the message arrives; a peer that refreshes cannot read an older value |
| one shared subscription per JVM | no per-region subscription lag or connection-pool starvation |
| heartbeat (2 s interval, 6 s timeout) | detects a silently dead subscription that would otherwise miss invalidations forever |
| gap clear + `reconnectPending` | after a detected gap, no local hit is trusted until the subscription is confirmed live |
| `near-cache.recovery-grace-ms` (8 s) | after recovery, reads keep going to the database while in-flight writer timestamps settle |
| `near-cache.ttl-ms` | the only bound when a `PUBLISH` succeeds but the message is lost, or when a connection dies without an error (default `0` = disabled) |
| fail-closed on Redis outage | during an outage the window is not "stale", it is "database read" |

Two honest caveats remain:

1. With `ttl-ms=0`, a lost `PUBLISH` on an otherwise healthy connection can leave a
   peer stale until the key is written again. Set a finite TTL if that matters.
2. A silently blackholed data connection that never returns an error and is never
   exercised (all reads hit the mirror) will not be detected. A finite TTL forces
   periodic `HGET`s and closes that gap.

---

## Race conditions and how they are avoided

| Race | Mechanism |
|---|---|
| A peer reads while a writer has an uncommitted transaction | **SoftLock marker** `NO_DATA:<owner>:<createdAt>` in the global map; peers evict and fail closed. The owner recognises its own marker through `locallyLockedKeys` + `instanceId` and keeps its own state. |
| The writer crashes while holding the lock | the marker carries `createdAt`; `isExpiredLock` abandons it after `LOCK_TTL_MS` (60 s) so the entry is not blocked forever. |
| A peer processes an invalidation before the new timestamp is visible | `HSET` is synchronous and happens **before** `PUBLISH`; when the invalidation arrives the new timestamp is already there. |
| A pod receives its own invalidation echo | messages carry the sender's `instanceId`; the publisher ignores its own message because its local state was updated in the same operation. |
| A DB fill is mistaken for a committed write | `storeWrite` uses the local lock set: value **with** the lock = commit (bump timestamp); value **without** the lock = fill (adopt timestamp). Without this, every DB load would invalidate every other pod. |
| A fill adopts a timestamp that is older than the row it read | known window (see caveats): capture the global timestamp before the DB read and use it for the fill. This is the remaining narrow race in the timestamp algorithm. |
| Recovery clear wipes an in-flight transaction's pending value | `evictDataExcept(locallyLockedKeys)` preserves the payloads of keys this node currently owns a SoftLock for; everything else is cleared. |
| Duplicate or out-of-order invalidations | invalidation is idempotent: the peer removes local state; the next read asks Redis for the authoritative timestamp rather than trusting message order. |
| A commit and a read of the same key on the same pod | the pod sees its own lock marker and own pending value; validation returns "valid" only for its own lock, and the local payload is the value it just wrote. |
| Write-write conflict during a Redis outage | not solved by the cache: without Redis there is no distributed SoftLock. The database's transaction isolation is the authority; reads still fail closed and recovery clears the cache. |

---

## End-to-end in six steps

1. **Cold read:** mirror miss → `HGET` → no timestamp → miss → database load →
   store payload locally, write the timestamp to Redis, publish invalidation.
2. **Hot read:** mirror hit → compare with the payload's local timestamp → equal →
   serve from Caffeine. Zero network calls.
3. **Write:** SoftLock marker `HSET` + `PUBLISH` → database commit → new timestamp
   `HSET` + `PUBLISH` → local payload updated.
4. **Peer invalidation:** the message removes the peer's mirror entry and payload;
   its next read re-reads Redis, sees the newer timestamp, and reloads from the DB.
5. **Redis down:** the first failed data command marks the node unavailable; every
   entity read now misses to the database; fills and commits are not cached; query
   timestamps also miss.
6. **Recovery:** the node clears all local timestamps and payloads, forces DB
   revalidation for the grace window, then the zero-hop hot path resumes.

The detailed phases below walk through every step with concrete keys, Redis
commands, and the code that performs them.

---

## 1. The cast

| Component | Module | Responsibility |
|---|---|---|
| `HybridCacheProcessor` | `cache-extension-deployment` | Build-time wiring: region factory, beans, native-image reflection |
| `HybridCacheRecorder` | `cache-extension-runtime` | Contributes the region-factory properties at static and runtime init |
| `QuarkusHybridCacheRegionFactory` | `cache-extension-runtime` | `RegionFactory`: creates payload/timestamp storage per region |
| `CaffeineStorageAccess` | `cache-extension-runtime` | The actual payload cache (entity values) |
| `TimestampRegistry` | `cache-extension-runtime` | Distributed timestamps, lock markers, availability, invalidation callback |
| `QuarkusDistributedTimestampMap` | `cache-extension-runtime` | The global map: Redis hash + local mirror + pub/sub registration |
| `HybridCachePubSub` | `cache-extension-runtime` | The single owner of the pub/sub channel: one shared subscription **and** the non-blocking invalidation publisher |
| `HybridCacheRedisProvider` | `cache-extension-runtime` | Holds the Quarkus `RedisDataSource` / `ReactiveRedisDataSource` (declares the injection points) |
| `AbstractTimestampAwareStorage` | `cache-extension-runtime` | The local map, the comparison, the lock handling |
| `DomainDataTimestampsRedisStorage` | `cache-extension-runtime` | Entity / collection regions |
| `NaturalIdTimestampsRedisStorage` | `cache-extension-runtime` | Natural-id regions |
| `DomainQueryTimestampsRedisStorage` | `cache-extension-runtime` | Query timestamps region (no payload tier) |
| `DistributedCacheLockWrapper` | `cache-extension-runtime` | Lets a stored SoftLock be unwrapped before Hibernate sees it |

---

## Phase 1 — Build time

This happens in Quarkus' **augmentation** phase, once per build, not per pod.

1. The processor's `feature()` registers the feature name
   (`hybrid-distributed-cache`).
2. `integrateWithHibernateOrm` (`STATIC_INIT`):
   - creates a static-init listener via `HybridCacheRecorder`;
   - that listener contributes the region-factory class and cache flags:
     ```properties
     hibernate.cache.region.factory_class = <backend region factory>
     hibernate.cache.use_second_level_cache = true
     hibernate.cache.use_query_cache        = true
     ```
3. `integrateWithHibernateOrmRuntime` (`RUNTIME_INIT`):
   - contributes the **same** properties again, because the Quarkus Hibernate
     extension installs its own local-cache factory in the runtime settings and
     would otherwise win;
   - adds the recorder's nested `HybridRegionFactoryInitiator`, which re-creates
     the `RegionFactory` from those properties and strips `null` config values
     that would otherwise cause an NPE.
4. **Redis backend:** `keepRedisClientBeans()` marks `RedisDataSource` /
   `ReactiveRedisDataSource` unremovable, and `registerRedisProvider()` registers
   `HybridCacheRedisProvider` and `HybridCachePubSub` as unremovable beans. This
   forces the Quarkus Redis extension to create its client beans.
5. `registerForReflection()` registers the region factory for native-image.

**Takeaway:** the build phase only wires configuration and beans. No cache data,
no Redis connection.

---

## Phase 2 — Pod startup

1. Quarkus starts; the static and runtime init listeners run, so the Hibernate
   settings are in place.
2. Hibernate builds the `SessionFactory` and calls `prepareForUse(...)` on
   `QuarkusHybridCacheRegionFactory`.
3. For each cache region Hibernate knows about, it asks the factory for storage:

   ```
   Entity region        → CaffeineStorageAccess + TimestampRegistry
                          → DomainDataTimestampsStorage
   Natural-id region    → same, but NaturalIdTimestampsStorage
   Collection region    → DomainDataTimestampsStorage
   Query results region → plain CaffeineStorageAccess (payload only)
   Timestamps region    → CaffeineStorageAccess + TimestampRegistry
                          → DomainQueryTimestampsStorage
   ```

   The `TimestampRegistry` is `QuarkusDistributedTimestampMap`.

4. Each `AbstractTimestampAwareStorage` constructor registers itself as the
   invalidation listener of its region's timestamp registry.
5. **Redis:** `HybridCachePubSub`'s `@PostConstruct` starts its heartbeat
   scheduler (every 2 s by default) only when pub/sub is enabled. With
   `quarkus.hybrid-cache.pubsub.enabled=false`, no subscriber and no scheduler
   are started at all.
6. **Lazy initialization.** The Redis provider declares the
   `RedisDataSource` / `ReactiveRedisDataSource` injection points; no connection
   is created during startup. `init()` runs on the first cache operation:
   - it caches the `HGET`/`HSET` facades and registers with
     `HybridCachePubSub`, which lazily `SUBSCRIBE`s to
     `hybridcache.invalidation`.

7. **One owner for pub/sub.** `HybridCachePubSub` owns both directions of the
   Redis channel. Subscribing, the heartbeat and every invalidation `PUBLISH` use
   the reactive client; nothing on the write path waits for a pub/sub round trip.
   The synchronous Redis hash commands still use the blocking client because
   Hibernate's `StorageAccess` SPI is synchronous (see
   [Reactive applications](#reactive-applications-and-threading)).

After startup: beans exist, regions exist, and (when pub/sub is enabled) the
scheduler exists, but there is no network traffic yet.

---

## Phase 3 — First read of an entity (cold)

The walk-through below uses the Redis names (`QuarkusDistributedTimestampMap`,
`HGET`/`HSET`).

A request asks for customer `CUST-001`, and it is not in the JPA session.

```
Request
  → Hibernate checks the persistence context (1st-level cache): miss
  → Hibernate asks the region: DomainDataTimestampsStorage.getFromCache(key)
      → validateCacheEntryBasedOnTimeStamp(key)
          → getRemote(key.toString())
              → init() on first use (Redis client + subscription)
              → local mirror: miss
              → HGET <region> "CUST-001"  → null
          → remoteTimeStamp == null
          → remove any local timestamp, evict local payload (nothing there)
          → return "miss"
  → Hibernate queries the database
  → Hibernate stores the result: putFromLoad(key, entity)
      → storeFromLoad(key, entity)
          → getRemote(key.toString()) → HGET → still null
          → no global timestamp yet
          → localDataStorage.putIntoCache(key, entity)        // Caffeine
          → updateTimeStampInAll(key)
              → localTimestamps[key] = now
              → remoteKeyTimestampCache.put(key, now)
                  → HSET <region> "CUST-001" <now>
                  → PUBLISH "hybridcache.invalidation" "<region>|<podId>|CUST-001"
  → entity returned
```

Network cost: **2 × HGET + 1 × HSET + 1 × PUBLISH**.

Two details worth calling out:

- The second `HGET` exists because a fill must **adopt** an existing global
  timestamp rather than create a new one. Creating a new one on every DB read
  would make pods invalidate each other forever.
- The mirror is updated by the `HSET` path (`nearCachePut`), so the next read
  does not need Redis.

---

## Phase 4 — Second read (hot)

```
Request
  → Hibernate checks the persistence context: miss (new session)
  → getFromCache(key)
      → validateCacheEntryBasedOnTimeStamp(key)
          → getRemote("CUST-001")
              → local mirror: HIT → return timestamp (0 network)
              → mustRevalidate()? no
              → TTL expired? no (TTL is 0 by default)
          → not a lock marker
          → timeFromRemote == timeFromLocal
          → valid → return null
      → getLocalValue(key) → Caffeine HIT → entity
  → entity returned
```

Network cost: **0**. No Redis, no database. This is the hot path, and it is why
the local mirror exists.

If the *same* JPA session asks twice, even the L2 cache is not touched —
Hibernate returns the entity from the persistence context.

---

## Phase 5 — The SoftLock window

Timestamps only change on **commit**. Consider pod A updating `CUST-001`:

```
t0: global timestamp = 100   (A's local = 100, B's local = 100)
t1: A starts updating the row, but has NOT committed yet
t2: B reads CUST-001
    → global(100) <= local(100) → "valid" → B serves the OLD row
```

That is a dirty read relative to A's uncommitted transaction. Worse: if B
re-caches the old row and sets its local timestamp to `now`, B's stale copy can
outlive A's commit.

The SoftLock fills the gap. On lock acquisition Hibernate calls
`putIntoCache(key, SoftLock)`, and the extension puts a **lock marker** into the
global map:

```
HSET <region> "CUST-001" "NO_DATA:<podA>:<acquireTime>"
PUBLISH ... "CUST-001"
```

Now B's read sees "locked" and refuses the cache instead of serving the old row.

Why a string marker instead of the real `SoftLock`? Because the global map here
is a plain Redis hash serialized as JSON. A `SoftLock` is a Hibernate-internal
object. The extension instead stores a marker plus:

- `locallyLockedKeys` — "this pod owns this lock";
- an `instanceId` — "the marker belongs to this pod";
- a timestamp inside the marker — "older than 60 s means the owner died;
  abandon it".

That gives the same safety without serializing Hibernate internals.

---

## Phase 6 — Update on pod A (the full write)

Hibernate uses the READ_WRITE strategy, so the sequence is
**acquire → write → commit → release**.

### 6a. Lock acquisition

```
Hibernate: putIntoCache(key, SoftLock)
  → storeWrite
      → value is a SoftLock
      → isReleased(lock)? no (unlockTimestamp == 0)
      → locallyLockedKeys.add(key)
      → Caffeine.put(key, new DistributedCacheLockWrapper(lock))
      → localTimestamps.remove(key)
      → remoteKeyTimestampCache.put(key, NO_DATA:<podA>:<now>)
          → HSET + PUBLISH
```

The `HSET` is synchronous, so the lock marker is globally visible before the
write continues; the `PUBLISH` is queued on the reactive client and adds no
round trip to the transaction. Other pods receive the invalidation shortly after:
it removes the key from their mirror and evicts their payload. Their next read
does `HGET`, sees the lock marker, and misses → DB read. Their `putFromLoad` also
sees the lock marker and **refuses to cache** the value.

What pod A sees during the transaction: the marker is its own →
`locallyLockedKeys` contains the key → the validator returns "valid", and
`getLocalValue` unwraps the stored wrapper and hands Hibernate its own
`SoftLock`.

### 6b. Database write

The `UPDATE` is flushed / committed to the database. Nothing cache-side yet.

### 6c. Commit into the cache

```
Hibernate: putIntoCache(key, updatedEntity)
  → storeWrite
      → not a SoftLock
      → locallyLockedKeys.remove(key) == true   → a real write, not a fill
      → Caffeine.put(key, updatedEntity)
      → updateTimeStampInAll(key)
          → localTimestamps[key] = now
          → HSET <region> "CUST-001" <newNow>
          → PUBLISH ... "CUST-001"
```

This is the moment the global timestamp moves. The `HSET` is synchronous, while
the `PUBLISH` is fired asynchronously, so the commit does not wait for a pub/sub
round trip. Other pods get the invalidation shortly after and drop their mirror
entry + payload.

### 6d. Lock release

If Hibernate calls `putIntoCache(key, releasedSoftLock)`:

```
isReleased(lock) == true
  → locallyLockedKeys.remove(key)
  → pendingTimestamp = localTimestamps.get(key)   // the commit timestamp from 6c
  → if present: remote.put(key, pendingTimestamp) // HSET + PUBLISH again
  → else (rollback): evict Caffeine, remove local timestamp, HDEL + PUBLISH
```

Rollback therefore leaves no marker behind: no committed value means the
timestamp is removed entirely, and other pods fail closed on their next read.

---

## Phase 7 — Pod B after the update

```
1. Pub/sub message arrives on the shared channel
   → HybridCachePubSub.dispatch parses "<region>|<sender>|<key>"
   → ignores its own messages
   → calls the region's onPubSubMessage
       → localMap.remove(key)              // mirror entry gone
       → notifyInvalidation(key)
           → onRemoteInvalidation(key)
               → evict Caffeine payload (by stringified key)
               → localTimestamps.remove(key)

2. Next getFromCache(key)
   → getRemote → mirror miss → HGET → new timestamp
   → remote > local (local was removed) → miss
   → Hibernate reads the database
   → putFromLoad → storeFromLoad
       → HGET → remote is a Number
       → Caffeine.put(key, fresh entity)
       → localTimestamps[key] = that same remote timestamp   // adopt, do not bump
   → subsequent reads are hot again
```

The important part: pod B did **not** need to map the string key back to a
Hibernate key object, because the eviction happened lazily inside a read that
already had the real key. (The eager listener also evicts, and that is what needs
`evictDataByStringKey` — the O(1) string-key index in `CaffeineStorageAccess`.)

---

## Phase 8 — Delete / evict / clear

| Hibernate call | What happens |
|---|---|
| `removeFromCache(key)` | evict payload + remove both timestamps (`HDEL` + `PUBLISH`) |
| `evictData(key)` | same as above |
| `evictData()` (whole region) | clear Caffeine + clear local timestamps + `DEL <region>` + publish `CLEAR_ALL_CACHE`. Every pod clears its mirror and payloads on receipt. In-flight local SoftLocks are preserved (see below). |
| `release()` | release the payload tier (evict all) |

---

## Phase 9 — Natural-id and query cache

- **Natural-id region:** `NaturalIdTimestampsStorage`. Same logic, different
  Hibernate key type. A natural-id lookup first resolves `CUST-001` → primary key
  through this region, then the primary-id load uses the entity region. One
  request can therefore touch two regions, and both get timestamps.
- **Query results region:** the result payload lives in local Caffeine only.
- **Timestamps region:** `DomainQueryTimestampsStorage` keeps query-space
  timestamps in the **global Redis map** — read straight from Redis on every
  lookup, written with `HSET` on updates. Hibernate compares those timestamps to
  decide whether a cached query result is still usable. Cross-pod query
  invalidation therefore rides the same Redis hash, not payload replication.

### Verified query-cache behaviour

`QueryCacheDemoResource` runs this cacheable JPQL query:

```java
select c.customerCode from Customer c where c.name = :name
        order by c.customerCode
```

`GET /cache/query/demo/{name}` executes it twice with the Hibernate
`org.hibernate.cacheable` hint and `em.clear()` in between, returning the two
result sets plus Hibernate's query-cache counters. With the default
`reset=true`, it clears the statistics and evicts the query results region first,
so the counters describe this call only. `?reset=false` leaves the existing
query cache and statistics in place.

`QueryCacheTest` asserts two things:

| Scenario | Expected | Observed |
|---|---|---|
| First run (reset=true) | 1st execution misses, 2nd hits | `misses=1`, `hits=1`, `puts>=1`; both results `[CUST-001]` |
| After `PUT /customers/CUST-001` | cached result is stale, query re-executes | `reset=false` → `firstResult=[]`, `misses>=1`, `hits>=1` |

After the update, Redis contains the query-space entry (keyed by the table
name, not the entity name):

```
redis-cli HGETALL default-update-timestamps-region
  customer   7348862210351104
```

That value comes from Hibernate's `SimpleTimestamper.next()`, which is
`System.currentTimeMillis() << 12` (1 ms = 4096 units, bumped monotonically).
So query-space timestamps are **monotonic**, unlike the entity timestamps the
extension writes itself with `System.currentTimeMillis()`.

Application suite: `CacheStrategyTest` (5), `HybridCacheTest` (3),
`QueryCacheTest` (2), `TimestampValidationTest` (8) — 18 tests, 0 failures.

Redis runtime unit suite: `HybridCacheConfigTest` (2),
`CaffeineStorageAccessTest` (7), `TimestampValidationTest` (9),
`HybridCachePubSubPublishTest` (2),
`QuarkusDistributedTimestampMapNearCacheTest` (8) — 28 tests, 0 failures.

---

## Phase 10 — Failure modes

### Redis is down

1. The first failing **data** operation (`HGET`/`HSET`/`HDEL`/`DEL`) sets
   `remoteAvailable = false` and `retryAt = now + retryDelayMs` (5 s). A failed
   `PUBLISH` does not, because it must not disable the data path.
2. During the cooldown, `canUseRedis()` short-circuits → `getRemote` returns
   null → `isRemoteAvailable()` is false → the validator evicts the local payload
   and reports a miss. **Reads fail closed to the database**: without the
   timestamp authority no payload can be proven current.
3. Cache fills are skipped while Redis is unavailable, so the local tier is not
   repopulated with unverifiable payloads. Query timestamps already behave this
   way.
4. When Redis returns, `onRedisSuccess()` clears the failure flag, discards the
   local timestamp mirror and payloads, and arms
   `forceRevalidateUntil = now + recoveryGraceMs` (8 s): reads deliberately
   ignore local state and reload, because the node may have missed peer writes
   while Redis was down. This clear is unconditional; there is no strategy that
   keeps entries cached during an outage.

### Pub/sub is down but Redis is up

1. The heartbeat publishes a token every 2 s; if no echo arrives within 6 s,
   `HybridCachePubSub` declares the subscription lost.
2. It clears every region's near-cache once (missed invalidations), then retries
   every 2 s.
3. Once a message proves the new subscription is live, it clears again and the
   maps arm the revalidation window. Until then `reconnectPending` forces reads
   to revalidate from Redis.
4. A failed invalidation `PUBLISH` is tracked separately from the data path: it
   is logged (WARN once, then DEBUG) but never sets `remoteAvailable = false`, so
   a broken or ACL-denied `PUBLISH` cannot stop `HGET`/`HSET` from working. Only
   a heartbeat failure changes the subscription state.

### A pod dies holding a SoftLock

The marker contains `<createdAt>`. After `LOCK_TTL_MS` (60 s) `isExpiredLock`
returns true: the marker is removed, the payload evicted, and the entry reloads.
The cache does not stay blocked forever.

### Clock skew

Timestamps are `System.currentTimeMillis()`. The comparison assumes pods' clocks
are close (NTP). A pod whose clock is far ahead can make its writes look "newer"
than they are, and a pod far behind can look stale. This is the main
environmental assumption.

### Redis memory at scale

The Redis footprint is **not** bounded by
`quarkus.hybrid-cache.storage.maximum-size` (that is the local Caffeine payload
per pod). It grows with the number of **distinct entity keys any pod has ever
cached**, and nothing expires or compacts it:

- every entity / natural-id / collection key gets one hash field;
- there is **no `EXPIRE`** anywhere in the runtime module — `HSET` on write,
  `HDEL` only on an explicit Hibernate evict/remove;
- Caffeine's size-based eviction does not notify Redis (no removal listener), and
  neither does a pod shutdown, so entries outlive the payload they describe;
- the query timestamps region is different: one field per **query space**
  (table name), so it is bounded by the schema size.

Measured on this repo's Redis (10 000 fields, `MEMORY USAGE`):

| Region | Field | Bytes/field |
|---|---|---|
| entity | `com.example.app.Customer#<id>` | ~87 B |
| natural-id | `com.example.app.Customer##NaturalId<value>` | ~96 B |

So a natural-id entity costs roughly **180 B** in Redis, independent of payload
size (the payload is local):

| Distinct rows cached | Redis (entity + natural-id) |
|---|---|
| 1 M | ~180 MB |
| 10 M | ~1.8 GB |
| 100 M | ~18 GB |

Pod count does **not** multiply this — the hash is shared and keyed by entity.
Pod count multiplies pub/sub fan-out (one `PUBLISH` delivered to N subscribers),
not storage. The dangerous growth driver is a **bulk scan/report**: it loads
millions of distinct keys, each gets a timestamp, and Caffeine then throws almost
all payloads away while the Redis fields remain.

What happens when it fills depends on the server config:

| `maxmemory-policy` | Behaviour | Verdict |
|---|---|---|
| `maxmemory 0` (current local setup) | grows until the host OOMs / disk fills (persistence) | outage |
| `noeviction` + limit | `HSET` fails → `remoteAvailable=false` → writes stay local; peers never get the timestamp or pub/sub message | **silent staleness** |
| `allkeys-lru` + limit | entity timestamps evicted → `remoteTimeStamp == null` → fail closed, reload from DB | safe, but stampede |
| `allkeys-lru` + limit | query-space timestamp evicted → `isSpaceOutOfDate` returns `false` when the timestamp is missing (verified in Hibernate 6.6 bytecode) | **stale query results** |

Operational notes:

- In a Redis Cluster, a region is **one hash = one key = one slot**, so a large
  region cannot be spread across shards. Scaling out therefore requires manual
  hash sharding (`region:0..N`) or one key per entry.
- `evictData()` uses `DEL <region>`; on a multi-million-field hash this blocks
  Redis while it frees memory. `UNLINK` is the non-blocking alternative.
- If `AOF`/`RDB`/replicas are enabled, the dataset is duplicated 2–3×.

Mitigations, cheapest first:

1. A `maxmemory` limit with `allkeys-lru` and an alert at ~70 % is recommended;
   running unlimited with `noeviction` must be avoided.
2. `HEXPIRE` on write (Redis ≥ 7.4; this container is 8.10.2) can expire entity
   timestamps after a few hours and refresh them on write. The query timestamps
   region must **not** be TTLed (it is small, and its absence makes caches look
   fresh).
3. A Caffeine `removalListener` that `HDEL`s the key on size eviction ties Redis
   to the working set, at the cost of cross-pod reload churn.
4. L2 caching for bulk scans should be avoided (only hot entities should be
   cached); bulk scans are the real source of key cardinality.
5. If this is still too much, the invalidation grain can be switched from
   per-key to per-region (one timestamp per region): O(regions) in Redis, at the
   cost of invalidating the whole region on every write.

---

## Deployment topologies (standalone / Sentinel / Cluster)

The extension never opens its own connection: `HybridCacheRedisProvider` injects
Quarkus' `RedisDataSource` / `ReactiveRedisDataSource`, and the only constant is
the channel name `hybridcache.invalidation`. Topology is therefore **configuration
only** — `quarkus.redis.client-type` (default `standalone`), plus `hosts`,
`master-name`, `role`, `auto-failover`, TLS and auth.

The data path is topology-neutral: `HGET`/`HSET`/`HDEL` on **one** hash key,
`DEL` on **one** key, `PUBLISH`/`SUBSCRIBE` on **one** channel. No `MULTI`,
no Lua, no `SCAN`/`KEYS`, no multi-key command, so there is no `CROSSSLOT`
exposure.

### Sentinel

```properties
quarkus.redis.client-type=sentinel
quarkus.redis.master-name=mymaster
quarkus.redis.hosts=redis://sentinel-1:26379,redis://sentinel-2:26379,redis://sentinel-3:26379
quarkus.redis.auto-failover=true          # default is false
quarkus.redis.password=...

quarkus.hybrid-cache.near-cache.ttl-ms=30000
```

- `auto-failover` defaults to **`false`**; without it a promoted replica is not
  followed.
- Quarkus documents that auto-failover applies to command connections but **not
  to pub/sub subscriptions**. On failover the subscription dies and the
  extension's heartbeat (2 s interval, 6 s timeout) detects it, clears the near
  caches, re-subscribes and forces revalidation for `recoveryGraceMs` (8 s).
  The expected result is an ~8 s window of database reads, not stale data.
- Replication is asynchronous: a promoted replica may be missing the last
  timestamp writes. Because every pod detects the pub/sub break and revalidates,
  the entry is reloaded from the database; size the RPO with
  `min-replicas-to-write` if the window matters.

### Cluster

```properties
quarkus.redis.client-type=cluster
quarkus.redis.hosts=redis://node-1:6379,redis://node-2:6379,redis://node-3:6379
```

Correctness is unaffected, but **each region is a single hash = a single key =
a single slot**, so:

- all timestamps for a region live on one shard (capacity + hotspot), and
- `DEL <region>` on a multi-million-field hash blocks that shard (`UNLINK`).

Sharding the hash (`region:0..N` chosen by key hash) is the code change needed to
spread a region; it also complicates region clear. Regular (non-sharded) pub/sub
is broadcast cluster-wide, so invalidation still reaches every pod.

### Replication and compatible servers

`client-type=replication` works (with `role`); the first host is the master when
the topology is static. KeyDB / Valkey / Dragonfly / ElastiCache / MemoryDB /
Azure Cache all work because only basic RESP commands are used.

Two caveats:

- **Proxies** (Twemproxy, Envoy) and HTTP-only endpoints: basic commands work,
  but `PUBLISH`/`SUBSCRIBE` often do not, and a RESP/TCP endpoint is required.
- If pub/sub is unavailable, the near-cache mirror must expire on its own:
  set `quarkus.hybrid-cache.near-cache.ttl-ms` (e.g. 30000). With the default
  `0` and no invalidation messages, a pod keeps its stale mirror forever —
  timestamps cannot compensate, because the pod never re-reads the global one.

---

## Call-count summary

| Operation | Redis calls |
|---|---|
| Hot read (mirror hit) | 0 |
| Cold read | 2 `HGET` + 1 `HSET` + 1 `PUBLISH` |
| Read after another pod's write | 2 `HGET` |
| Write with lock | lock `HSET+PUBLISH` + commit `HSET+PUBLISH` (+ release if called) |
| Delete | 1 `HDEL` + 1 `PUBLISH` |
| Region clear | 1 `DEL` + 1 `PUBLISH` |
| Background | 1 `PUBLISH` heartbeat per 2 s per JVM + one shared `SUBSCRIBE` |

`PUBLISH` is issued on the reactive client and is fire-and-forget: it is counted
as a Redis command but does not add a round trip to the write or read that
triggered it. `HGET`/`HSET`/`HDEL`/`DEL` are synchronous because Hibernate's
cache SPI is synchronous.

---

## The 60-second summary

1. **Build:** the extension replaces Hibernate's region factory and wires
   Redis + Hibernate.
2. **Startup:** per cache region the extension builds a Caffeine payload cache
   and a timestamp map; Redis connects lazily on first use.
3. **First read:** global timestamp is missing → miss → load from DB → store
   payload locally and write the timestamp to Redis (and broadcast).
4. **Second read:** compare the local timestamp with the mirror of the global
   one → equal → serve from Caffeine. Zero network.
5. **Update:** lock marker goes into Redis first (so peers stop trusting their
   copy), then the new value and a new timestamp are published; peers evict
   lazily on their next read.
6. **Coherence:** correctness is the timestamp comparison; pub/sub is the eager
   optimization that keeps the timestamp mirror fresh. If Redis fails, reads go
   to the database until it returns; if pub/sub fails, reads revalidate against
   Redis. Recovery always clears local state.
7. **SoftLock:** because timestamps move only on commit, the lock marker covers
   the in-flight window and prevents dirty reads and stale overwrites.

---

## Assumptions and caveats

- **Fill race:** a fill that races a remote commit can still store a value read
  from the database **before** that commit, with a local timestamp set **after**
  it. The window is the duration of the database read. The robust fix is to
  capture the global timestamp *before* the DB read and use that as the fill's
  local timestamp.
- **Write detection:** `storeWrite` distinguishes a committed write from a cache
  fill by the local lock set. If Hibernate ever writes a value without a prior
  lock, the value is treated as a fill (the global timestamp is adopted, not
  bumped).
- **Clocks:** the algorithm assumes reasonably synchronized pod clocks.
- **Key identity:** the Redis hash field is `key.toString()`, so correctness
  depends on Hibernate's cache key `toString()` being stable and unique.
- **Memory:** `localTimestamps` and `locallyLockedKeys` are unbounded per region;
  they grow with the number of distinct cached keys. The payload tier's
  `keyIndex` (string key → Hibernate key) is bounded by the Caffeine
  `maximum-size` and is cleaned on size eviction, replacement, explicit evict and
  clear.

### Reactive applications and threading

The extension deliberately splits its Redis traffic by operation semantics:

| Operation | Client | Blocking? | Why |
|---|---|---|---|
| `HGET` / `HSET` / `HDEL` / `DEL` | `RedisDataSource` | yes | Hibernate's `StorageAccess` is synchronous and must return a value |
| `SUBSCRIBE` / heartbeat / `PUBLISH` | `ReactiveRedisDataSource` | no | fire-and-forget, must never block a reactive request thread |

Consequences for applications:

- **Invalidation publishing never blocks.** It is fired on the reactive client
  and the write returns immediately; a failed publish is logged (once at WARN,
  then at DEBUG) and does **not** mark the data connection as unavailable.
  `PUBLISH` failures therefore cannot stop `HGET`/`HSET` from working.
- **Cache reads/writes inherit Hibernate ORM's threading model.** They are
  blocking and must run on a worker thread. With RESTEasy Reactive, either keep
  the Hibernate endpoint blocking (return a plain type / annotate `@Blocking`)
  or offload it explicitly (`Uni.createFrom().item(...).runSubscriptionOn(...)`,
  `@RunOnVirtualThread`, …). Calling blocking Hibernate ORM from the event loop
  is an application bug that no cache extension can make safe; the blocking
  Redis client reports it as `BlockingNotAllowedException`.
- **No hidden threads on the hot path.** The only background thread is the
  single pub/sub heartbeat scheduler; publish, subscribe and dispatch are
  event-driven.

### Performance notes

- The invalidation `PUBLISH` is no longer part of write latency.
- `HybridCachePubSub` caches its `ReactivePubSubCommands` facade instead of
  allocating one per heartbeat / invalidation.
- Each timestamp map caches its `HashCommands` / `KeyCommands` facades.
- A pub/sub invalidation evicts the payload through an O(1) string-key index in
  `CaffeineStorageAccess`; it no longer scans the whole Caffeine key set.
- A failed `PUBLISH` and a failed data command are tracked independently, so one
  broken direction of Redis cannot disable the other.

---

## Running and testing

The application suite needs Redis on `localhost:6379`:

```bash
docker run -d --name redis -p 6379:6379 redis
./gradlew :application:test
```

Redis runtime unit tests do not need a server:

```bash
./gradlew :cache-extension-runtime:test
```

### Failure and recovery semantics

- A failed Redis command marks the connection unavailable and starts the
  `redis.retry-delay-ms` cooldown. Reads fail closed to the database meanwhile:
  the local payload tier is not served and is not repopulated.
- A pub/sub gap clears every local near-cache once and forces revalidation for
  `near-cache.recovery-grace-ms`, because invalidations published during the gap
  were lost.
- `AbstractTimestampAwareStorage.onRemoteInvalidation(null)` deliberately **does
  not** clear `locallyLockedKeys`: a recovery broadcast can arrive while this node
  is inside a READ_WRITE transaction, and forgetting ownership of the SoftLock
  would make the transaction's committed value look like a peer write. The
  regression test is
  `TimestampValidationTest.recoveryInvalidationPreservesOwnedSoftLocksAndEvictsTheRest`.
- A failed `PUBLISH` is logged but does not mark the data connection unavailable;
  correctness still comes from the timestamp comparison on the next read.

### Operational notes

- **One subscription per JVM.** `HybridCachePubSub` shares a single Redis
  subscription across every Hibernate region; subscribing once per region would
  exhaust the client pool.
- **Non-blocking invalidation.** `PUBLISH` uses the reactive client and is
  fire-and-forget, so a write never waits for a pub/sub round trip.
- **Region clear.** `DEL <region>` removes the region hash, then `CLEAR_ALL_CACHE`
  is published so every pod clears its mirror and payloads.
