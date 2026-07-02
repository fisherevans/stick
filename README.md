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
keyboard. It runs on the existing homelab Proxmox cluster in its own LXC, and it
**generalizes what ramble-runner (CT 104) already does** for one app: run Claude
Code sessions in a memory-safe LXC (the CLI OOMs in a k8s pod,
[nottingham-cloud#121](https://github.com/fisherevans/nottingham-cloud/issues/121)),
under a lock that caps concurrency. ramble-runner's flock is a semaphore of one;
stick makes it a configurable pool of N and puts an authenticated streaming API in
front of it. Sessions bill against Fisher's Max subscription via
`CLAUDE_CODE_OAUTH_TOKEN`, not the per-token API - so a stick is a unit of
subscription concurrency, not cost.

It is distinct from the interactive operator-session path
([nottingham-cloud#125](https://github.com/fisherevans/nottingham-cloud/issues/125)),
which is a human driving a remote Claude Code session by hand on a separate host
(an old laptop, dev sessions only). The two are separate platforms on separate
hosts; they **share provisioning patterns** (LXC bootstrap, credential surface,
worktree-per-session), reused as code, not a shared machine.

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
