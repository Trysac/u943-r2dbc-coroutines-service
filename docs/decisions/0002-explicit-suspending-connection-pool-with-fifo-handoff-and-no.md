# Explicit Suspending Connection Pool with FIFO Handoff and NonCancellable Transaction Finalizers

Status: accepted (generated, pending human review)

## Context

Reactive services built with Kotlin coroutines and R2DBC risk subtle production outages when event-loop threads execute blocking work or when cancelled coroutines return half-finished connections to the pool or leave transactions open.

## Decision

Implement CoroutinePool using a Mutex-protected FIFO waiter queue and in-creation slot tracking with kotlinx-coroutines-core, discard connections upon cancellation, timeout, or non-ServiceException errors, scope NonCancellable strictly to COMMIT/ROLLBACK inside transaction {}, and provide EventLoopGuard with blocking {} to fail fast when invoked on registered event-loop threads.

## Consequences

Pool bounds, FIFO acquisition, query timeouts, and cancellation cleanup can be verified deterministically in virtual time using kotlinx-coroutines-test without external database containers; nested withConnection calls on the same coroutine acquire a second connection and require maxSize >= 2.
