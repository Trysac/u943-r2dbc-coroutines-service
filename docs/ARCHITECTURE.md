# Architecture

## Purpose

`io.github.trysac:r2dbc-coroutines-service` is a small, dependency-free Kotlin library: Coroutine data-access layer with a bounded suspending connection pool, cancellation-safe transactions, per-call timeouts and a guard that fails blocking calls on event-loop threads

## Shape

```text
caller code --> package io.github.trysac.r2dbc_coroutines_service (src/main/kotlin)
                    |
                    +-- objects and top-level functions that are pure; no I/O, no global mutable state
                    +-- internal or private helpers (optional)
```

- **Public API**: every public declaration of the package. `internal` and `private` declarations
  may change without a major version.
- **Errors**: invalid input throws an `IllegalArgumentException` subclass of the library (such as
  `BlankInputException`) documented with `@throws`. The library never prints or exits.
- **Side effects**: none. If a feature needs I/O, accept an `InputStream`, a `Reader` or a small
  interface instead of opening resources, and record the choice in an ADR.

## Quality contract

`scripts/Check.java` is the single definition of the gates; `fabrica.json` tells the factory and CI
how to run it. See [TESTING.md](TESTING.md).

## Decisions

Architecture decisions are recorded in [decisions/](decisions/) as numbered ADRs.
