# Hybrid Distributed Second-Level Cache for Quarkus

A Quarkus extension that replaces Hibernate ORM's second-level cache with a
three-tier design:

- **Tier 1 — local near-cache (JVM heap):** per-key timestamps mirrored from Redis,
  served without a network round trip on the hot path.
- **Tier 2 — Redis:** the authoritative timestamp registry (one hash per Hibernate
  region). Entity payloads are **not** stored in Redis.
- **Tier 3 — local payload cache:** Caffeine with Window TinyLFU eviction.

Cross-node coherence comes from timestamp comparison plus Redis pub/sub
invalidation. When Redis is unavailable, cache reads fail closed to the database:
the timestamp registry cannot prove a local payload current, so none is served.

```
read  → local timestamp hit? ──yes──→ local payload hit? ──yes──→ return (0 network hops)
                    │ no                        │ no
                    └────────→ HGET timestamp ──┴──→ database load → cache fill
write → SoftLock marker in Redis → commit → HSET timestamp + PUBLISH invalidation
```

## Modules

| Module | Purpose |
|---|---|
| `cache-extension-runtime` | Runtime behavior: region factory, storage, pub/sub, configuration |
| `cache-extension-deployment` | Build-time wiring with Quarkus and Hibernate ORM |
| `application` | Demo Quarkus app and integration tests |

### Runtime package layout

| Package | Contents |
|---|---|
| `com.hybridcache.runtime.nearcache` | Distributed timestamp map, pub/sub subscriber, Redis provider, `TimestampRegistry` — **no Hibernate imports** |
| `com.hybridcache.runtime.hibernate` | Hibernate ORM integration: region factory, storage adapters, Caffeine payload tier, lock wrapper |
| `com.hybridcache.runtime.config` | `@ConfigMapping` configuration (`quarkus.hybrid-cache.*`) |

The `nearcache` package only exposes `TimestampRegistry` to the Hibernate layer, so it
can be extracted into a standalone extension later without touching the cache provider.

## Requirements

- Java 21
- Quarkus 3.37.x
- Redis 7+ (`localhost:6379` for the demo application; the tests expect a local Redis)

## Build and test

```bash
./gradlew build
```

The `application` tests exercise the extension against H2 + Redis. Start Redis first:

```bash
docker run -d --name hybridcache-redis -p 6379:6379 redis:7-alpine
```

## Configuration

All properties are runtime configuration and live under the
`quarkus.hybrid-cache` prefix. Defaults are shown below.

| Property | Default | Description |
|---|---|---|
| `quarkus.hybrid-cache.pubsub.enabled` | `true` | Enable cross-node invalidation over Redis pub/sub |
| `quarkus.hybrid-cache.pubsub.reconnect-delay-ms` | `2000` | Delay before retrying a lost pub/sub subscription |
| `quarkus.hybrid-cache.pubsub.heartbeat-interval-ms` | `2000` | Heartbeat interval used to detect a dead subscription |
| `quarkus.hybrid-cache.pubsub.heartbeat-timeout-ms` | `6000` | Heartbeat timeout before the subscription is considered lost |
| `quarkus.hybrid-cache.near-cache.ttl-ms` | `0` | Optional staleness bound; `0` disables it |
| `quarkus.hybrid-cache.near-cache.recovery-grace-ms` | `8000` | Force revalidation of near-cache hits after a reconnect; `0` disables it |
| `quarkus.hybrid-cache.redis.retry-delay-ms` | `5000` | Cooldown before retrying Redis after a failed command |
| `quarkus.hybrid-cache.storage.maximum-size` | `10000` | Max payload entries per Hibernate cache region (Caffeine) |
| `quarkus.hybrid-cache.storage.record-stats` | `false` | Collect Caffeine hit/miss/eviction statistics |

Example `application.properties`:

```properties
quarkus.hybrid-cache.near-cache.ttl-ms=30000
quarkus.hybrid-cache.storage.maximum-size=20000
```

Local cache state is always discarded on recovery. There is deliberately no
option to keep it: entries cached while the timestamp registry was unreachable
cannot be proven current, so keeping them would allow stale reads.

## Usage

Add the runtime artifact to the application (once the extension is published):

```groovy
implementation 'com.hybridcache:cache-extension-runtime:1.0.0-SNAPSHOT'
```

The deployment module is resolved automatically through the Quarkus extension
descriptor. Enable Hibernate's second-level cache as usual:

```properties
quarkus.hibernate-orm.second-level-caching-enabled=true
```

## Consistency and failure model

- Reads are eventually consistent: an invalidation is delivered over pub/sub, and
  timestamps are compared before a local payload is returned.
- Redis failures fail closed: after a short cooldown the node serves entity reads
  from the database instead of the local payload tier.
- Any Redis or pub/sub recovery clears all local timestamp and payload state.
  There is no mode that keeps entries cached during an outage.
- Pub/sub gaps clear the local near-caches once and force revalidation for
  `near-cache.recovery-grace-ms` so writes missed during the outage are re-read.
- `syncStrategy = INVALIDATE`, `storeCacheMiss = false`: a Redis miss is never
  cached locally, so keys that appear later are picked up.

## Status

`experimental` — see `META-INF/quarkus-extension.yaml`. API and configuration may
change.

## Notes

- Configuration documentation is generated by the Quarkus annotation processor
  for Maven builds. With the current Gradle layout the processor cannot locate
  the module coordinates, so the build prints a warning and the table above is
  the source of truth.
