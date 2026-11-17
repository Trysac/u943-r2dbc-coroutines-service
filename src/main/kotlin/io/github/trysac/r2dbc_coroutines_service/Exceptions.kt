package io.github.trysac.r2dbc_coroutines_service

/**
 * Base exception for all predictable service and data-access errors.
 *
 * Every failure carries a stable machine-readable [code] (`blocking_on_event_loop`,
 * `sql_too_long`, `too_many_rows`, `query_timeout`, `pool_closed`, `acquire_timeout`,
 * or `invalid_argument`). Messages never include sensitive query payloads or more than
 * 200 characters of untrusted input.
 *
 * @property code stable machine-readable error code.
 * @param message human-readable description of the error.
 * @param cause underlying exception if available.
 */
public open class ServiceException(
    public val code: String,
    message: String = code,
    cause: Throwable? = null,
) : RuntimeException(sanitizeMessage(message), cause) {
    /**
     * Constants and message sanitization helpers for [ServiceException].
     */
    public companion object {
        private const val MAX_MESSAGE_LENGTH: Int = 200

        private fun sanitizeMessage(msg: String): String =
            if (msg.length <= MAX_MESSAGE_LENGTH) msg else msg.take(MAX_MESSAGE_LENGTH)
    }
}

/**
 * Exception thrown by [CoroutinePool] when acquiring a connection fails (for example,
 * with code `acquire_timeout`).
 *
 * @param code stable machine-readable pool error code.
 * @param message human-readable description of the pool failure.
 * @param cause underlying exception if available.
 */
public class PoolException(
    code: String,
    message: String = code,
    cause: Throwable? = null,
) : ServiceException(code, message, cause)
