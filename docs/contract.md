# The stick contract

This is the stable surface a consumer builds against. It defines how you
authenticate, how sessions and sticks (the semaphore) behave, the streaming turn
API and its event frames, the lifecycle, and the error/backpressure model. If it
isn't in here, don't depend on it.

Contract version: **v1** (all paths are under `/v1`). Additive changes (new event
types, new optional fields) stay within v1; breaking changes bump to `/v2` and the
old version keeps running until consumers migrate.

## Model

Four concepts. Get these and the rest follows.

- **Consumer** - an authenticated caller. One per app (bloom-bot is a consumer).
  Identified by a provisioned client credential. The consumer identity is the key
  for auth, per-consumer quota, and metrics tags.
- **Stick** - a session slot. There is a fixed pool of `N` sticks across the whole
  service. Holding a stick is permission to run one live session. Sticks are the
  semaphore: no free stick means you queue. A released or evicted session hands its
  stick back.
- **Session** - a warm, long-lived Claude Code agent bound to a consumer-supplied
  **session key**. The session holds one stick for its lifetime. Session state
  (the agent's working context, its checkout) is **disposable compute** - stick can
  evict it and the consumer must be able to reconstruct anything it cares about.
  **The consumer owns durable state**, not stick.
- **Turn** - one request/response exchange within a session. You send input text,
  stick streams back tokens, tool-execution events, and structured output until the
  turn completes. Turns within a session are sequential: one turn per session at a
  time.

### Session keys and warm reuse

A session key is any stable string the consumer chooses to mean "the same
conversation." bloom-bot uses the Discord thread id. The rules:

- First turn against a new key **creates** a session (acquiring a stick, or queuing
  if none are free).
- Subsequent turns against a live key **reuse** the warm session - same agent, same
  context, no re-acquire.
- An idle session is **evicted** after a timeout, handing its stick back. The next
  turn against that key transparently creates a fresh session (and may queue). A
  consumer should treat "my session was evicted" as normal, not an error - it only
  means in-agent context was lost, which is why durable state lives consumer-side.

Session keys are scoped per consumer. Two consumers using the same key string get
two independent sessions.

## Authentication

Per-consumer **provisioned client secret**. stick is internal and never publicly
exposed, so there is no OIDC/user-login flow - a consumer presents a static secret
that an operator provisioned into stick's registry.

```
Authorization: Bearer <client-secret>
```

- Every request carries it. Missing or unknown -> `401`.
- stick maps the secret to a consumer id (used for quota + metrics). The consumer
  never sends its id separately; it's derived from the credential.
- Secrets are provisioned out of band (an operator materializes them from Bitwarden
  into stick's config per the nottingham-cloud app-contract secrets rule). Rotation
  is an operator action; a consumer just gets a new secret value.

Consumers store their secret however they already store config/secrets. stick's
promise is that you present one bearer token and never manage an agent env
yourself.

## The streaming API (SSE)

Transport is **Server-Sent Events over HTTP**. The interaction is turn-based -
POST a turn, read a stream of frames back on the same response - which SSE models
directly, is trivial for a Go server and any HTTP client, and needs no extra
protocol machinery. There is no separate websocket/gRPC surface in v1.

All request and response bodies are JSON except the turn stream, which is
`text/event-stream`. All timestamps are RFC 3339. All ids are opaque strings.

### Open or reuse a session

You do not have to create a session explicitly - posting a turn to a key does it
for you. An explicit create exists for consumers that want to pre-warm or inspect:

```
POST /v1/sessions
{ "key": "discord:thread:1234", "idle_timeout_seconds": 900 }
```

Response `200`:

```json
{
  "key": "discord:thread:1234",
  "state": "active",          // "active" | "queued"
  "queue_position": 0,         // present when state == "queued"
  "created_at": "2026-07-02T16:00:00Z"
}
```

If no stick is free the session is **queued** (`state: "queued"`, with a
`queue_position`); it becomes `active` when a stick frees up. See Backpressure.

### Send a turn (the stream)

```
POST /v1/sessions/{key}/turns
Accept: text/event-stream
{ "input": "Summarize this article: ...", "metadata": { "any": "passthrough" } }
```

If the key has no live session, this creates one first (acquiring a stick or
queuing). The response is an SSE stream. Each event has a named `event:` and a JSON
`data:` payload:

| `event:` | when | `data` payload |
| --- | --- | --- |
| `queued` | the turn is waiting for a stick | `{ "queue_position": 3 }` - may repeat as the position drops |
| `turn_started` | a stick is held and the agent began the turn | `{ "turn_id": "t_abc", "session_key": "..." }` |
| `token` | incremental assistant output | `{ "text": "partial text" }` - concatenate in order |
| `tool_start` | the agent began a tool call | `{ "tool": "web_fetch", "tool_call_id": "tc_1", "title": "Fetching article" }` |
| `tool_end` | a tool call finished | `{ "tool_call_id": "tc_1", "status": "ok" \| "error", "summary": "..." }` |
| `structured_output` | a structured result the agent emitted | `{ "name": "article_summary", "value": { ... } }` |
| `turn_completed` | the turn finished cleanly | `{ "turn_id": "t_abc", "stop_reason": "end_turn" }` |
| `error` | the turn failed | `{ "code": "...", "message": "..." }` - terminal, stream then closes |

Guarantees:

- Exactly one terminal event ends the stream: `turn_completed` or `error`. After it,
  the connection closes.
- `token` events are ordered; concatenating their `text` yields the full assistant
  message. A consumer that only wants final text can buffer tokens and ignore the
  tool events.
- `tool_start`/`tool_end` let a consumer show pending state ("researching...").
  Every `tool_start` gets a matching `tool_end` with the same `tool_call_id` before
  `turn_completed`. This is the surface P3 enriches; in the MVP the set of tools and
  the `title`/`summary` text are best-effort and a consumer must tolerate unknown
  tool names.
- `structured_output` is how a consumer gets machine-readable results without
  parsing prose. The `name` namespaces the payload; `value` is arbitrary JSON the
  agent produced. A turn may emit zero or more.
- If the turn was queued, the stream opens immediately with one or more `queued`
  events and only then proceeds to `turn_started` once a stick is acquired. The
  consumer can render an hourglass off the first `queued` event.

Heartbeats: stick sends SSE comment lines (`: ping`) periodically so idle proxies
don't drop the connection. Clients ignore comment lines per the SSE spec.

### Release a session

```
DELETE /v1/sessions/{key}
```

Ends the session and hands its stick back immediately. Idempotent - deleting an
unknown or already-evicted key is `204`, not an error. Consumers should release
when they know a conversation is done (e.g. a thread archived) rather than waiting
for idle eviction, so the stick returns to the pool sooner. Releasing is optional;
idle eviction is the backstop.

### Inspect (optional)

```
GET /v1/sessions/{key}   -> 200 session object, or 404
GET /v1/pool             -> { "sticks_total": N, "sticks_in_use": k, "queue_depth": q }
```

`GET /v1/pool` lets a consumer surface global pressure if it wants; it is not
required for normal operation.

## Lifecycle summary

```
              first turn / explicit create
consumer  ───────────────────────────────────►  acquire a stick
                                                   │
                            no stick free  ────────┤
                                                   ▼
                                              QUEUE (queued events / queue_position)
                                                   │  stick frees up
                                                   ▼
                                              ACTIVE session (holds one stick)
                                                   │
             send-turn ──► stream frames ──► turn_completed        (repeat, warm reuse)
                                                   │
                    idle_timeout elapsed  ─────────┤  or  DELETE /v1/sessions/{key}
                                                   ▼
                                              EVICTED (stick handed back)
```

Warm reuse is the fast path: a live session answers turns without touching the
semaphore. The semaphore is only consulted on create/acquire and release/evict.

## Backpressure

When all sticks are in use, new session acquisition **queues** rather than failing.
The consumer sees this two ways, and should surface it (bloom-bot shows an
hourglass):

- On explicit `POST /v1/sessions`: response `state: "queued"` with a
  `queue_position`.
- On `POST .../turns`: the stream leads with `queued` event(s) carrying
  `queue_position`, then transitions to `turn_started` when a stick is acquired.

`queue_position` is best-effort and monotonic-ish (it can only be an estimate under
concurrency). A consumer uses it for display, not for logic. There is no hard queue
cap in v1; if one is added it surfaces as a `429` with `Retry-After` on acquire and
is documented here before it ships.

## Errors

JSON error bodies on non-stream endpoints, and the `error` SSE event on streams,
share one shape:

```json
{ "code": "stick_error_code", "message": "human-readable, safe to log" }
```

| HTTP / event | `code` | meaning |
| --- | --- | --- |
| `401` | `unauthenticated` | missing/unknown bearer secret |
| `403` | `quota_exceeded` | consumer over its provisioned quota |
| `404` | `no_such_session` | key has no live session (on GET; turns auto-create instead) |
| `409` | `turn_in_progress` | a turn is already streaming for this session; turns are sequential |
| `429` | `queue_full` | acquire rejected because the queue is capped (only if a cap exists) |
| `500` | `internal` | stick-side failure |
| `error` event | `agent_failed` | the agent errored mid-turn; stream terminates |
| `error` event | `evicted` | the session was evicted mid-turn (rare; treat like a lost turn, retry) |

Retries: a `token` stream that dies without a terminal event should be treated as a
failed turn and retried from the start of the turn (turns are not resumable in v1).
Because durable state is consumer-side, replaying a turn is safe.

## Observability

stick emits DogStatsD to the nottingham-cloud metrics hub (arr-matey,
`10.10.10.31:8125`) with an explicit `host:` tag, per the homelab
instrument-on-the-way-in rule. The contract-relevant metrics, all tagged by
`consumer`:

- `stick.sessions.active` (gauge) - live sessions.
- `stick.pool.in_use` / `stick.pool.total` (gauge) - stick utilization.
- `stick.queue.depth` (gauge) - sessions waiting for a stick.
- `stick.turn.latency` (timing) - turn start-to-complete.
- `stick.turns.count` (count) - tagged by `stop_reason`.

These back the platform's dashboards and alerts; a consumer doesn't need them, but
they're part of the service contract in the sense that the platform is observable by
construction.

## What a consumer can rely on, in one paragraph

Present a bearer secret. POST a turn to a session key; read an SSE stream of
`token` / `tool_*` / `structured_output` frames ending in `turn_completed` or
`error`; render `queued` as an hourglass. Reuse the same key for a continuing
conversation and it stays warm; expect it to be evicted when idle and be ready to
start over, because your durable state lives on your side, not stick's. That's the
whole surface.
