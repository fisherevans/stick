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
- **Stick** - a concurrency slot for one **turn**. There is a fixed pool of `N`
  sticks across the whole service. Running a turn requires holding a stick; it is
  held only for the duration of that turn and released the instant it ends. Sticks
  are the semaphore: if all `N` are busy when you send a turn, the turn queues
  until one frees. `N` is sized to how many turns can run at once (a turn is where
  the compute happens), not how many sessions exist.
- **Session** - a warm Claude Code agent bound to a consumer-supplied **session
  key**. A session is cheap while idle: between turns there is no running process,
  so an idle session holds **no stick** and costs ~nothing. Many warm sessions can
  coexist under a small pool; only their *simultaneous* turns contend. Session
  state (the agent's working context) is **disposable compute** - stick can evict
  an idle session and the consumer must be able to reconstruct anything it cares
  about. **The consumer owns durable state**, not stick.
- **Turn** - one request/response exchange within a session. You send input text,
  stick streams back tokens, tool-execution events, and structured output until the
  turn completes. Turns within a session are sequential: one turn per session at a
  time.

### Session keys and warm reuse

A session key is any stable string the consumer chooses to mean "the same
conversation." bloom-bot uses the Discord thread id. The rules:

- First turn against a new key **creates** a session, then acquires a stick to run
  that turn (queuing if all sticks are busy).
- Subsequent turns against a live key **reuse** the warm session - same agent, same
  context - and each acquires a stick just for its own duration.
- An idle session is **evicted** after a timeout (it holds no stick, so nothing is
  handed back - it just frees its working state). The next turn against that key
  transparently creates a fresh session. A consumer should treat "my session was
  evicted" as normal, not an error - it only means in-agent context was lost, which
  is why durable state lives consumer-side.

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
for you. An explicit create exists for consumers that want to pre-warm:

```
POST /v1/sessions
{ "key": "discord:thread:1234", "tools": [ ... ], "system_prompt": "...", "model": "opus" }
```

The body may carry the full **session configuration** (see "Session configuration"
below): `tools`, `system_prompt`, `model`, `disallowed_tools`, and `seed`. Config
binds to the session at creation and holds
for its warm life; stick remembers it per key and re-applies it if the session is
idle-evicted and recreated, so a consumer sets it once and it survives a rehydrate.

Response `200`:

```json
{
  "key": "discord:thread:1234",
  "state": "active",
  "created_at": "2026-07-02T16:00:00Z"
}
```

Explicit create **blocks until a stick is acquired** and returns `state:"active"`
(it is a synchronous pre-warm). It does not stream, so it is not where queue
backpressure surfaces - that's the turn stream (below), which leads with `queued`
frames when it has to wait. Use explicit create only if you want a session warm
before the first turn; most consumers just post a turn.

### Send a turn (the stream)

```
POST /v1/sessions/{key}/turns
Accept: text/event-stream
{ "input": "Summarize this article: ...", "tools": [ ... ], "system_prompt": "..." }
```

`input` is required. The turn may also carry any **session configuration** field
(same set as create). Config is bound when the turn *creates* the session; on a
turn against an already-warm session the config fields are ignored (config is
fixed for a warm session's life). Send it on the turns that may create a session -
a client that always sends it keeps its persona/tools/seed across a rehydrate.

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
| `turn_completed` | the turn finished cleanly | `{ "turn_id": "t_abc", "stop_reason": "end_turn", "usage": { ... } }` |
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
- `turn_completed` carries a `usage` object accounting for the turn, so a consumer
  can meter its own spend:

  ```json
  { "model": "claude-sonnet-5", "input_tokens": 2860, "output_tokens": 14,
    "cache_read_input_tokens": 23055, "cache_creation_input_tokens": 5354,
    "cost_usd": 0.0478, "duration_ms": 4853 }
  ```

  Token counts are the durable signal. `cost_usd` is Anthropic's list-price
  estimate the runtime reports; it is nonzero even under stick's Max subscription
  token (where there is no per-request charge) and becomes real spend only if stick
  is ever run against an API key. The stub agent omits `usage`; a consumer must
  tolerate its absence.
- If the turn was queued, the stream opens immediately with one or more `queued`
  events and only then proceeds to `turn_started` once a stick is acquired. The
  consumer can render an hourglass off the first `queued` event.

Heartbeats: stick sends SSE comment lines (`: ping`) periodically so idle proxies
don't drop the connection. Clients ignore comment lines per the SSE spec.

### Declaring tools (structured output)

A consumer that needs **structured** results - not prose it has to parse - declares
**output tools**. Each is a named tool with a JSON input schema; the agent calls it
with a structured argument, and stick surfaces that argument to the consumer as a
`structured_output` frame. This is the reliable path: the runtime validates the call
against the schema and the agent can emit it mid-turn as a native action, so it holds
up for multi-step workflows where a trailing text block would not.

Declare tools on `POST /v1/sessions` or on the first turn's `tools` field:

```json
{
  "tools": [
    {
      "name": "emit_node",
      "description": "Return the node: a short read plus 3-5 options the reader can pick.",
      "output_name": "node",
      "input_schema": {
        "type": "object",
        "properties": {
          "read": { "type": "string" },
          "options": { "type": "array", "items": {
            "type": "object",
            "properties": { "label": {"type":"string"}, "lead_in": {"type":"string"} },
            "required": ["label", "lead_in"] } }
        },
        "required": ["read", "options"]
      }
    }
  ]
}
```

When the agent calls `emit_node`, the consumer receives:

```
event: structured_output
data: { "name": "node", "value": { "read": "...", "options": [ ... ] } }
```

Notes:
- `output_name` is the `structured_output` `name` (defaults to the tool name). It lets
  you call the tool `emit_node` but decode a frame named `node`.
- Tools are exposed to the underlying Claude Code session over MCP; the agent sees them
  as native, schema-validated tools. Prompt the agent to call the tool (e.g. "when done,
  call `emit_node` with the node").
- Output-tool calls do **not** produce `tool_start`/`tool_end` frames (they're the
  result channel, not user-facing tool activity). Other tools the agent uses still do.
- **The argument is validated against `input_schema` in-band.** If the agent calls
  the tool with an argument that violates the schema, stick's tool rejects the call
  with the specific validation errors (which field, what's wrong), so the agent
  corrects and re-calls within the same turn - the same loop as any CLI that demands
  valid structured input. Only a schema-valid call becomes a `structured_output`
  frame, so a consumer's declared schema is a real guarantee, not a hint. Declare
  precise schemas (types, `required`, enums) and let the runtime enforce them.
- Round-trip **callback tools** (the agent calling back into the consumer mid-turn for
  data) are a planned extension on this same surface (fisherevans/stick#9); output tools
  are the one-way subset.

#### Required output (guaranteed structured result)

Set `"required": true` on an output tool to make its result a guarantee rather than a
hope. stick steers the turn to call the tool (it injects a directive so the model
delivers its result through the tool, not as prose), and if the turn still ends without
it, stick runs a bounded, invisible repair turn on the same session asking for it. The
consumer's contract becomes: **declare a required output tool → you get exactly one
`structured_output` frame with that name, or an explicit `error`** (`output_not_produced`)
if even the repair could not produce it. This removes the consumer-side "did the model
remember to call the tool?" retry logic.

Reliability tracks model capability: a capable model (the default) calls a required tool
essentially every turn once directed; a small/fast model is less consistent and may fall
through to the `output_not_produced` error. Prefer a capable `model` for required output.

### Session configuration

Beyond `tools`, the create/turn body may carry these fields. All are optional, all bind
at session creation, and all are remembered per key so an idle-evicted session recreates
with them (a transparent rehydrate):

| field | effect |
| --- | --- |
| `system_prompt` | Replaces the agent's default system prompt, so the session runs as the consumer's **persona** (e.g. a summarizer), not a generic coding agent. This is how a caller makes the LLM behave as its product expects. |
| `model` | Per-consumer model override (e.g. `"opus"`), instead of the platform default. Set once per session, not per turn. |
| `seed` | Grounding context prepended to the session's **first** turn (e.g. reference material the whole session reasons over). Remembered, so it is re-applied if the session is recreated. |
| `disallowed_tools` | Tool policy: a **denylist** of tools to remove (e.g. `["ReportFindings","ToolSearch"]` to drop noise tools while keeping Bash/WebSearch and the output tools). A denylist is used rather than an allowlist because an allowlist (the CLI's `--tools`) drops the consumer's MCP output tools on resume turns; a denylist keeps every tool and just removes the named ones. |
| `allowed_tools` | Optional permission allowlist passed through to the agent (e.g. `"Bash(git *)"`); rarely needed since sessions run with permissions bypassed. |

### Release a session

```
DELETE /v1/sessions/{key}
```

Ends the session and frees its working state immediately. Idempotent - deleting an
unknown or already-evicted key is `204`, not an error. Releasing is optional (idle
eviction is the backstop); it mainly frees session state sooner. It does not need
to return a stick, because an idle session isn't holding one.

### Inspect (optional)

```
GET /v1/sessions/{key}   -> 200 session object, or 404
GET /v1/pool             -> { "sticks_total": N, "sticks_in_use": k, "queue_depth": q }
```

`GET /v1/pool` lets a consumer surface global pressure if it wants; it is not
required for normal operation. `sticks_in_use` is the number of turns running right
now (not the number of sessions), and `queue_depth` is turns waiting for a stick.

## Lifecycle summary

```
   POST turn ──► WARM session (no stick while idle)
                     │
                     ├─► acquire a stick for THIS turn ──┐
                     │        all sticks busy? ──► QUEUE (queued frames) ──┘
                     ▼
                 turn_started ──► stream frames ──► turn_completed ──► release the stick
                     │
                     │  (repeat; session stays warm, next turn re-acquires)
                     ▼
             idle_timeout / DELETE ──► EVICTED (session state freed; no stick to return)
```

A session doesn't touch the semaphore just by existing; each turn borrows a stick
for its own duration. So `N` warm sessions cost nothing when idle - contention only
happens when more than `N` of them run a turn at the same instant.

## Backpressure

When all sticks are busy, a new **turn** **queues** rather than failing. The
consumer sees this on the turn stream and should surface it (bloom-bot shows an
hourglass): `POST .../turns` leads with `queued` event(s) carrying a
`queue_position`, then transitions to `turn_started` once a stick is acquired.

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
| `error` event | `output_not_produced` | a turn declared a required output tool and neither the turn nor the repair pass produced it (see "Required output"); terminal |
| `error` event | `evicted` | the session was evicted mid-turn (rare; treat like a lost turn, retry) |

Retries: a `token` stream that dies without a terminal event should be treated as a
failed turn and retried from the start of the turn (turns are not resumable in v1).
Because durable state is consumer-side, replaying a turn is safe.

## Observability

stick emits DogStatsD to the nottingham-cloud metrics hub (arr-matey,
`10.10.10.31:8125`) with an explicit `host:` tag, per the homelab
instrument-on-the-way-in rule. The contract-relevant metrics, all tagged by
`consumer`:

Shipped agentless to Datadog (v2 series HTTP API - the box runs no Datadog agent),
enabled when `STICK_DD_API_KEY` is set. Pool/session/host gauges are sampled on the
flush interval; per-turn metrics are recorded as each turn completes, tagged
`consumer` + `model` (+ `status` on the count):

- `stick.pool.sticks_total` / `stick.pool.sticks_in_use` / `stick.pool.queue_depth`
  (gauge) - stick utilization and contention.
- `stick.sessions.live` (gauge) - warm sessions.
- `stick.turn.count` (count, tags `consumer`/`model`/`status`) - turn volume and
  error rate.
- `stick.turn.duration_ms` / `stick.turn.queue_wait_ms` (gauge) - turn latency and
  time spent waiting for a stick.
- `stick.turn.tokens.{input,output,cache_read,cache_write}` (count) and
  `stick.turn.cost_usd` (count) - usage and estimated cost per consumer/model.
- `stick.turn.max_rss_kb` (gauge) - peak RSS of the turn's claude process
  (resource pressure).
- `stick.host.mem_available_mb` / `stick.host.load1` (gauge) - box-level pressure
  and competing processes on the LXC.

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
