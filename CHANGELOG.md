# Changelog

All notable changes to durably are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Until 1.0, minor versions may change the API
or the database schema; such changes and their migration steps are listed here.

## [Unreleased]

### Added
- Workflow engine: workflows replay against an append-only event history in Postgres;
  recorded activity results are returned instead of re-run.
- Task claiming with `FOR UPDATE SKIP LOCKED` and epoch fencing on every history write.
- Activity workers with retries (exponential backoff and jitter), timeouts and a stable
  idempotency key (`workflow_id:activity_seq`).
- `Client.start` (idempotent by key), `Client.result`, `Client.describe`, `Client.history`.
- CLI: `durably init-db`, `durably worker --kind workflow|activity`.
- `durably.testing.FakeLLM` for tests.
- Failpoints (`FAILPOINT=<name>` / `<name>:pause`) for crash-testing workers.
- `durably.testing.SharedFakeLLM`: a fake provider whose counters live in Postgres, so
  they are shared across worker processes and survive crashes.
- Crash-recovery test suite with real worker processes: resume after a crash, a
  failpoint matrix, and a frozen-worker test with fencing on and off.
- Activity heartbeats: `await durably.heartbeat(details)` extends the lease;
  `heartbeat_timeout=` on `ctx.activity` retries hung attempts; the next attempt reads the
  last details from `durably.activity_info().heartbeat_details`.
- Durable timers: `await ctx.sleep(seconds)` survives crashes and deploys; each timer
  fires exactly once. Workflow workers fire due timers.
- Signals: `client.signal(id, name, payload)` and `await ctx.wait_signal(name, timeout=)`.
  Signals are received in send order, each exactly once; signalling a finished workflow
  raises `WorkflowNotRunningError`.
- `ctx.now()`, `ctx.random()` and `ctx.uuid4()`: values are recorded as markers the first
  time, so every replay sees the same value.
- Continue-As-New: `ctx.should_continue_as_new()` turns True once a run's history reaches
  `--max-history` events (default 1000); `ctx.continue_as_new(input)` starts a fresh run,
  carrying over untaken signals. `client.result` and `client.signal` follow the chain;
  `client.runs(id)` lists every run.

### Fixed
- A deadlock between delivering an event (activity result, timer, signal) and claiming
  the same workflow's task: all writes now take task locks before workflow locks.
