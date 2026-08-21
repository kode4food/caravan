# Caravan

## Project

In-process generic Go message streams. Module:
`github.com/kode4food/caravan`.

## Exact Scope

- Implement requested change. Preserve everything outside explicit scope.
- Never rename, move, redesign, delete, inline, add compatibility, or perform
  adjacent cleanup unless request requires it.
- Explain required broad consequences before action.
- Complete whole requested boundary. Never shrink or reframe request.

## Architecture

Dependencies flow from public API toward contracts, then implementations.

- `caravan`: fluent API; public results such as `Pair`, `Grouped`.
- `stream`, `topic`, `table`, `message`, `closer`: public contracts,
  lifecycle semantics.
- `internal/stream`: stream assembly, execution.
- `internal/stream/node`: processor implementations only.
- `internal/topic`, `internal/table`: storage implementations, invariants.
- `internal/sync`: narrow synchronization concepts.

Rules:

1. Give each concept one authoritative owner.
2. Export signatures using receiving public package language only.
3. Keep public streaming API fluent. Keep `Bind`, `Process`, node constructors,
   composition plumbing internal.
4. Name fluent stages as actions; constructors and types as nouns:
   `TableUpdate(updater)`, `NewTableUpdater`, `table.Updater`.
5. Add interfaces only for real lifecycle or substitution seams.
6. Name packages and files by concern. Never create `util`, `helpers`,
   `common`, `models`, or `types` dumping grounds.
7. Group files by concern: combine, grouping, buffering, timing, tables,
   lifecycle. Size alone never justifies a split or move.
8. Place fluent API tests in `caravan`; direct processor tests in
   `internal/stream/node`.
9. Move code only after confirming owner and fewer caller concepts.
   Forwarding wrappers never justify a boundary.

## Args and Results

- Functions with 5+ arguments use an `Args` struct.
- Functions returning 3+ data values use a `Res` struct. Trailing `bool` or
  `error` does not count as data.
- Name same-typed values with distinct roles. Add a struct when call sites
  become clearer.
- Declare single-use `Args` or `Res` immediately before its function. Pass by
  value. Never store or forward it.
- Give multi-call-site structs descriptive names without `Args` or `Res`.
- Pass plain structs up to 32 bytes by value; over 64 bytes by pointer. Use
  pointers for identity or mutation.

## Go Style

### Naming

- Use one lowercase receiver letter based on type role.
- Use short nearby locals: `ok` for lookups/assertions, `err` for errors.
  Use longer API-boundary names.
- Use verb-noun functions. Use `New` constructors, normally returning pointers.
- Omit type arguments Go can infer. Keep result-only or otherwise
  non-inferable arguments.
- When public contract package shares scope, alias implementation package as
  `_stream`, `_table`, or `_topic`.
- Keep acronyms uppercase: `ID`, `URL`, `HTTP`.
- Put `Err`-prefixed sentinels in an error `var` block.
- Follow analogous standard-library names.

### Declarations and Files

- Declare all `type` and `const` values at package scope, including tests.
- Put types before other declarations in owning concern file.
- Order top level: types, constants, variables, exported constructors,
  exported methods, exported functions, unexported methods, unexported
  functions.
- Keep related methods together in call-chain order.
- Run `goimports` on Go files.
- Keep Go code and comments within 80 columns. Soft-wrap Markdown prose.

### Structs and Control Flow

- Use named fields in struct literals except single-field wrappers.
- Prefer guard clauses. Keep conditional nesting to one level unless early
  return duplicates work.
- Use multi-assignment only for multiple results from one call.
- Store mutable counters, caches, registries, and lifecycle state on owning
  structs.
- Reference another package's exported value directly; never re-export it
  through a package-level variable.

### Comments

- Give every exported declaration Godoc: at most 3 lines, describing what.
- Comment unexported declarations only for non-obvious reasons: at most 2
  lines.
- End Godoc without a period.
- Use inline comments for why only.

### Errors

- Use typed package-level sentinels. Wrap as `%w: context`.
- Handle errors immediately.
- Panic only in documented `Must...` helpers whose contract promises it.

## Performance-Critical Topic Log

Measured hot path. Completed immutable segments deliberately disable locking.
Segment pooling is deliberate.

- Preserve `InitialMutex`, lock disabling, segment pooling, cursor behavior,
  and log concurrency. Never remove or weaken them without representative
  before/after benchmarks using thousands of consumers.
- Benchmark publication, consumption, cursor traversal, vacuuming, segment
  reuse with `-benchmem`.
- Base decisions on `ns/op`, `B/op`, `allocs/op`; code size proves nothing.

## Testing

- Maintain 90%+ coverage.
- Use external `package_test` packages.
- Use `testify/assert` only. Omit assertion messages.
- Keep test names short. Put scenarios in concise `t.Run` names.
- Use table-driven tests for multiple scenarios. Call `t.Helper()` in helpers.
- Iterate with `go test ./... -short`. Completion requires full `make test`.

## Git

- Preserve reviewed index exactly.
- Never commit unless current request explicitly asks.
- Never stage, unstage, reset, restore, or stash unless explicitly asked.
