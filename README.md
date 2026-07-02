# stick

A headless, multi-tenant platform service that hands out authenticated,
semaphore-limited, streamed Claude Code agent sessions to programmatic consumers.

The name is the metaphor: to run a session you must be holding a **stick**. There
are a fixed number of sticks. The count of sticks *is* the semaphore. Ask for a
stick when none are free and you queue until one is handed back. It's the talking
stick, applied to compute.

## Why this exists

Fisher's local apps (bloom-bot, and others to come) want to run Claude Code agents
without each one standing up its own agent runtime, credential surface, and
concurrency control. stick centralizes that: a consumer authenticates, opens a
session, streams a turn, and gets back incremental tokens, tool-execution events,
and structured output. The consumer owns its durable state; stick owns the
disposable compute and the concurrency budget.

stick is an **internal platform service** - zero public exposure, no human at the
keyboard. It is distinct from the interactive operator-session path
([nottingham-cloud#125](https://github.com/fisherevans/nottingham-cloud/issues/125)),
which is a human driving a remote Claude Code session by hand. The two **share the
operator-LXC substrate** (provisioning, credential surface, worktree-per-session
isolation) but are separate platforms with separate entry points.

## The contract

The API and lifecycle a consumer builds against are specified in
[docs/contract.md](docs/contract.md). That document is the stable surface; read it
before writing a client. The first consumer is bloom-bot (a Discord article-reader
bot in [fisherevans/bloom](https://github.com/fisherevans/bloom) under `bloom-bot/`).

## Status

Early. See the [milestones and issues](https://github.com/fisherevans/stick/issues)
for what's built and what's planned. The phases:

- **P0** - publish the contract spec (this, plus [docs/contract.md](docs/contract.md)).
- **P1** - pool + semaphore + auth + streaming API (headless text sessions on the
  operator substrate) and a reference client.
- **P2** - warm session lifecycle + queue backpressure + observability.
- **P3** - tool-event streaming for rich consumers.
