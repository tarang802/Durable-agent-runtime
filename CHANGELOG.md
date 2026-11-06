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
