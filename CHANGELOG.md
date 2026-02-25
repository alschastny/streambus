# Changelog

## Unreleased

### Added

- **Idempotent publishing** (requires Redis 8.6+) — set `idmpMode` in `StreamBusSettings` to `IdmpMode::Auto` or `IdmpMode::Explicit` to deduplicate messages from the same producer. Pass a `$producerId` to `add()`/`addMany()`, and wrap messages in `StreamBusMessage` to supply explicit idempotent IDs.
- **`StreamBusMessage`** — optional message wrapper that lets you set a custom entry ID and/or an idempotent ID when publishing.
- **Delete policy** (requires Redis 8.2+) — set `deletePolicy` in `StreamBusSettings` to control what happens when entries are removed from the stream:
  - `DeleteMode::KeepRef` (default) — PEL references in other consumer groups are preserved.
  - `DeleteMode::DelRef` — PEL references in all consumer groups are removed along with the entry.
  - `DeleteMode::Acked` — the entry is only deleted once every consumer group has acknowledged it.
- **`StreamBusProducer` now requires a producer name** passed to `createProducer(string $name)`, which is forwarded automatically on every publish.

### Changed

- `deleteOnAck` uses the atomic `XACKDEL` command on Redis 8.2+ instead of a separate `XACK` + `XDEL`, respecting the configured `deletePolicy`. On older servers the previous behavior is preserved.

## v0.1.0

Initial public release.
