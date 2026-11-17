# r2dbc-coroutines-service

Coroutine data-access layer with a bounded suspending connection pool, cancellation-safe transactions, per-call timeouts and a guard that fails blocking calls on event-loop threads.

## Problem

Reactive services written with coroutines and R2DBC stall when one handler makes a blocking call on the event loop, and leak connections when a cancelled request leaves a transaction open. These bugs pass ordinary unit tests and appear only under load. This library provides a suspending connection pool, transactions that always commit or roll back, timeouts, and a guard that turns a blocking call on an event-loop thread into an immediate, testable failure.

## Usage

```kotlin
import io.github.trysac.r2dbc_coroutines_service.*

val pool = CoroutinePool(factory = { /* your ConnectionFactory */ }, maxSize = 4)
val service = OrderService(pool)
runBlocking {
    val orderId = service.create("customer-42", 2500L)
    val orders = service.listByCustomer("customer-42")
    pool.close()
}
```

## Public API Summary

- **`CoroutinePool`**: Bounded pool with `maxSize` (default 10), `acquireTimeout` (default 5s), `queryTimeout` (default 30s), and `maxRows` (default 10,000).
- **`transaction`**: Extension function for atomic operations.
- **`EventLoopGuard`**: Registry for non-blocking event-loop threads; `blocking { }` enforces safe execution.
- **`OrderService`**: Example service demonstrating transactional CRUD.
- **Errors**: `ServiceException` with codes: `blocking_on_event_loop`, `sql_too_long`, `too_many_rows`, `query_timeout`, `pool_closed`, `acquire_timeout`, `invalid_argument`.

## Why not r2dbc-pool?

Unlike standard R2DBC pooling, this library provides explicit cancellation semantics, strict blocking-call enforcement on event-loop threads, and deterministic testing capabilities on virtual time.

## Security

See [docs/SECURITY.md](docs/SECURITY.md) for the threat model and resource limits.
