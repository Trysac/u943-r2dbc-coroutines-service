# Security & Threat Model

## Resource Exhaustion

- **Connection Pool**: Limits total open and in-creation connections to `maxSize` (default 10).
- **SQL Bound**: Rejects SQL exceeding 65,536 characters (see `CoroutinePool.MAX_SQL_LENGTH`).
- **Result Set Bound**: Rejects result sets exceeding 10,000 rows (see `CoroutinePool.DEFAULT_MAX_ROWS`).
- **Timeout**: Queries exceeding 30s (default) abort with `query_timeout`.

## Threat Mitigation

- **Event-Loop Starvation**: The `EventLoopGuard` prevents thread blocking on critical event loops by throwing `ServiceException("blocking_on_event_loop")`.
- **Error Sanitization**: `ServiceException` suppresses sensitive query parameters and truncates all error messages to a maximum of 200 characters.
