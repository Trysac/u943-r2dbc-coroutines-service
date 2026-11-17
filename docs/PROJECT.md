# Project record

## Objective

Coroutine data-access layer with a bounded suspending connection pool, cancellation-safe transactions, per-call timeouts and a guard that fails blocking calls on event-loop threads.

## Scope

- In scope: The public API documented in `README.md` and `ARCHITECTURE.md`, providing a coroutine-native connection pool and transactional support.
- Out of scope: Spring Boot integrations, real R2DBC drivers, Flow-based streaming, and database-specific savepoint implementations.

## Effort metrics

| Metric | Value |
|---|---|
| Tokens (input / output) | 3039190 / 29946 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.9481 |

## Factory run u943 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-17 | generation | google/gemini-4-argon | yes | 6492 | 14567 | 0 | 0 | 0.3173 |
| 2026-11-17 | fix-1 | google/gemini-3.8-flash | no | 81040 | 2362 | 0 | 487806 | 0.1062 |
| 2026-11-17 | fix-2 | google/gemini-3.8-flash | no | 130138 | 2676 | 0 | 599805 | 0.1526 |
| 2026-11-17 | fix-3 | google/gemini-3.8-flash | no | 85954 | 3772 | 0 | 596128 | 0.1233 |
| 2026-11-17 | fix-4 | google/gemini-3.8-flash | no | 131795 | 3790 | 0 | 628606 | 0.1602 |
| 2026-11-17 | audit | google/gemini-3.8-flash | no | 84340 | 1037 | 0 | 189317 | 0.0813 |
| 2026-11-17 | docs | google/gemini-3.1-flash-lite | no | 17769 | 1742 | 0 | 0 | 0.0071 |

Total: $0.9481. Prices: vertex line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).
