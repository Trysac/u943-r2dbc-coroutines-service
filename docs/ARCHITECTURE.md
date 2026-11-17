# Architecture

## Overview

The library implements a deterministic coroutine layer over a database port shaped like the R2DBC SPI, consisting of `DbConnection` and `ConnectionFactory`.

## Core Components

1. **`CoroutinePool`**: Manages an `ArrayDeque` of idle connections and a FIFO queue of waiters protected by a `Mutex`. It automatically closes connections that time out, are cancelled, or throw exceptions.
2. **`GuardedConnection`**: Wraps `DbConnection` to enforce constraints (SQL length, row count, query timeout) before forwarding execution.
3. **`EventLoopGuard`**: A runtime registry for threads that must not block. It intercepts calls via the `blocking { }` helper.
4. **Transactions**: The `transaction` extension uses `NonCancellable` exclusively for `COMMIT` and `ROLLBACK` to ensure resource cleanup.
