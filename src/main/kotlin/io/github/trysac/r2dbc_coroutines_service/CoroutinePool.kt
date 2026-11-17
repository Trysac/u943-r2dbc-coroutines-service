@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)

package io.github.trysac.r2dbc_coroutines_service

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Bounded suspending connection pool with strict FIFO acquisition, per-query timeouts,
 * SQL length and row-count bounds, and deterministic cancellation cleanup.
 *
 * ### Pool rules
 * - Total open connections (idle + active + currently being created) never exceed [maxSize].
 * - Waiting callers are served strictly first-in, first-out (FIFO).
 * - A connection whose call is cancelled, times out (`query_timeout`), or throws any
 *   non-[ServiceException] is closed immediately and never returned to the idle pool.
 * - Nested [withConnection] calls on the same coroutine acquire a second connection;
 *   callers should avoid nested acquisitions when [maxSize] is small to prevent self-deadlock.
 * - [close] marks the pool closed, rejects new and waiting acquisitions with `pool_closed`,
 *   and closes all currently idle connections.
 *
 * @param factory the [ConnectionFactory] used to create underlying connections.
 * @param maxSize maximum number of open connections at any point in time (must be >= 1).
 * @param acquireTimeout maximum duration to wait for a connection before throwing [PoolException] (`acquire_timeout`).
 * @param queryTimeout maximum duration for a single query before throwing [ServiceException] (`query_timeout`).
 * @param maxRows maximum rows a single query may return before throwing [ServiceException] (`too_many_rows`).
 */
public class CoroutinePool(
    private val factory: ConnectionFactory,
    private val maxSize: Int = DEFAULT_MAX_SIZE,
    private val acquireTimeout: Duration = 5.seconds,
    private val queryTimeout: Duration = 30.seconds,
    private val maxRows: Int = DEFAULT_MAX_ROWS,
) {
    private val mutex: Mutex = Mutex()
    private val idleConnections: ArrayDeque<DbConnection> = ArrayDeque()
    private val waiters: ArrayDeque<CompletableDeferred<Handoff>> = ArrayDeque()
    private val totalCreated: AtomicInteger = AtomicInteger(0)
    private var openOrCreatingCount: Int = 0
    private var closed: Boolean = false

    init {
        require(maxSize >= 1) { "maxSize must be at least 1, got $maxSize" }
        require(acquireTimeout.isPositive()) { "acquireTimeout must be positive" }
        require(queryTimeout.isPositive()) { "queryTimeout must be positive" }
        require(maxRows >= 1) { "maxRows must be at least 1, got $maxRows" }
    }

    private sealed interface Handoff {
        data class Existing(
            val connection: DbConnection,
        ) : Handoff

        data object CreateSlot : Handoff
    }

    private sealed interface AcquireAction {
        data class Ready(
            val connection: DbConnection,
        ) : AcquireAction

        data object CreateNew : AcquireAction

        data class Wait(
            val deferred: CompletableDeferred<Handoff>,
        ) : AcquireAction
    }

    /**
     * Returns the current number of idle connections sitting in the pool.
     */
    public fun idleCount(): Int = idleConnections.size

    /**
     * Returns the total number of physical connections created by [factory] since this pool started.
     */
    public fun createdCount(): Int = totalCreated.get()

    /**
     * Acquires a connection from the pool, runs [block] against a guarded view of that
     * connection, and either returns the connection to the pool or closes it on cancellation,
     * timeout, or unexpected failure.
     *
     * Note: Invoking [withConnection] inside another [withConnection] block on the same
     * coroutine acquires a second distinct connection from the pool.
     *
     * @param T the return value type of [block].
     * @param block suspending lambda receiving the checked [DbConnection].
     * @return the result produced by [block].
     * @throws PoolException with code `acquire_timeout` if no connection becomes available in time.
     * @throws ServiceException with code `pool_closed`, `sql_too_long`, `too_many_rows`, or `query_timeout`.
     */
    @Suppress("TooGenericExceptionCaught")
    public suspend fun <T> withConnection(block: suspend (DbConnection) -> T): T {
        val rawConnection = acquire()
        val guarded = GuardedConnection(rawConnection, queryTimeout, maxRows)
        var invalidate = false
        try {
            return block(guarded)
        } catch (e: ServiceException) {
            if (guarded.mustDiscard) {
                invalidate = true
            }
            throw e
        } catch (t: Throwable) {
            invalidate = true
            throw t
        } finally {
            if (invalidate || guarded.mustDiscard) {
                withContext(NonCancellable) {
                    discardConnection(rawConnection)
                }
            } else {
                withContext(NonCancellable) {
                    releaseConnection(rawConnection)
                }
            }
        }
    }

    /**
     * Closes the pool, fails any pending or subsequent acquisitions with [ServiceException]
     * (`pool_closed`), and closes all idle connections.
     */
    public suspend fun close() {
        val toClose: List<DbConnection>
        val toWake: List<CompletableDeferred<Handoff>>
        mutex.withLock {
            closed = true
            toClose = idleConnections.toList()
            openOrCreatingCount -= idleConnections.size
            idleConnections.clear()
            toWake = waiters.toList()
            waiters.clear()
        }
        val closedError = ServiceException("pool_closed", "Connection pool is closed")
        for (waiter in toWake) {
            waiter.completeExceptionally(closedError)
        }
        for (conn in toClose) {
            runCatching { conn.close() }
        }
    }

    @Suppress("ThrowsCount")
    private suspend fun acquire(): DbConnection {
        val action =
            mutex.withLock {
                if (closed) {
                    throw ServiceException("pool_closed", "Connection pool is closed")
                }
                val idle = idleConnections.pollFirst()
                when {
                    idle != null -> {
                        AcquireAction.Ready(idle)
                    }

                    openOrCreatingCount < maxSize -> {
                        openOrCreatingCount++
                        AcquireAction.CreateNew
                    }

                    else -> {
                        val deferred = CompletableDeferred<Handoff>()
                        waiters.addLast(deferred)
                        AcquireAction.Wait(deferred)
                    }
                }
            }

        return when (action) {
            is AcquireAction.Ready -> {
                action.connection
            }

            is AcquireAction.CreateNew -> {
                createWithReservedSlot()
            }

            is AcquireAction.Wait -> {
                val handoff =
                    try {
                        withTimeout(acquireTimeout) {
                            action.deferred.await()
                        }
                    } catch (e: TimeoutCancellationException) {
                        handleCancelledWaiter(action.deferred)
                        throw PoolException(
                            code = "acquire_timeout",
                            message = "Timed out after $acquireTimeout waiting for a connection",
                            cause = e,
                        )
                    } catch (e: CancellationException) {
                        handleCancelledWaiter(action.deferred)
                        throw e
                    }
                when (handoff) {
                    is Handoff.Existing -> handoff.connection
                    is Handoff.CreateSlot -> createWithReservedSlot()
                }
            }
        }
    }

    private suspend fun handleCancelledWaiter(deferred: CompletableDeferred<Handoff>) {
        withContext(NonCancellable) {
            val removed =
                mutex.withLock {
                    waiters.remove(deferred)
                }
            if (!removed && deferred.isCompleted && !deferred.isCancelled) {
                val handoff = runCatching { deferred.getCompleted() }.getOrNull()
                when (handoff) {
                    is Handoff.Existing -> releaseConnection(handoff.connection)
                    is Handoff.CreateSlot -> releaseReservedSlot()
                    null -> Unit
                }
            }
        }
    }

    private suspend fun releaseReservedSlot() {
        mutex.withLock {
            openOrCreatingCount--
            if (!closed) {
                var handedOff = false
                while (!handedOff && waiters.isNotEmpty() && openOrCreatingCount < maxSize) {
                    val waiter = waiters.pollFirst() ?: break
                    openOrCreatingCount++
                    if (waiter.complete(Handoff.CreateSlot)) {
                        handedOff = true
                    } else {
                        openOrCreatingCount--
                    }
                }
            }
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun createWithReservedSlot(): DbConnection {
        try {
            val conn = factory.create()
            totalCreated.incrementAndGet()
            return conn
        } catch (t: Throwable) {
            withContext(NonCancellable) {
                releaseReservedSlot()
            }
            throw t
        }
    }

    private suspend fun releaseConnection(conn: DbConnection) {
        var closeImmediately = false
        mutex.withLock {
            if (closed) {
                openOrCreatingCount--
                closeImmediately = true
                return@withLock
            }
            while (waiters.isNotEmpty()) {
                val next = waiters.pollFirst()
                if (next != null && next.complete(Handoff.Existing(conn))) {
                    return@withLock
                }
            }
            idleConnections.addLast(conn)
        }
        if (closeImmediately) {
            runCatching { conn.close() }
        }
    }

    private suspend fun discardConnection(conn: DbConnection) {
        try {
            runCatching { conn.close() }
        } finally {
            releaseReservedSlot()
        }
    }

    private class GuardedConnection(
        private val delegate: DbConnection,
        private val queryTimeout: Duration,
        private val maxRows: Int,
    ) : DbConnection {
        var mustDiscard: Boolean = false
            private set

        @Suppress("TooGenericExceptionCaught")
        override suspend fun execute(
            sql: String,
            params: List<Any?>,
        ): List<Map<String, Any?>> {
            if (sql.length > MAX_SQL_LENGTH) {
                throw ServiceException(
                    code = "sql_too_long",
                    message = "SQL length ${sql.length} exceeds maximum of $MAX_SQL_LENGTH characters",
                )
            }
            val rows =
                try {
                    withTimeout(queryTimeout) {
                        delegate.execute(sql, params)
                    }
                } catch (e: TimeoutCancellationException) {
                    mustDiscard = true
                    throw ServiceException(
                        code = "query_timeout",
                        message = "Query exceeded timeout of $queryTimeout",
                        cause = e,
                    )
                } catch (t: Throwable) {
                    mustDiscard = true
                    throw t
                }
            if (rows.size > maxRows) {
                throw ServiceException(
                    code = "too_many_rows",
                    message = "Result row count ${rows.size} exceeds maxRows of $maxRows",
                )
            }
            return rows
        }

        override suspend fun close() {
            mustDiscard = true
            delegate.close()
        }
    }

    /**
     * Constants and default thresholds for [CoroutinePool].
     */
    public companion object {
        /** Maximum allowed SQL query length in characters (65,536). */
        public const val MAX_SQL_LENGTH: Int = 65_536

        /** Default maximum number of connections in a pool. */
        public const val DEFAULT_MAX_SIZE: Int = 10

        /** Default maximum number of rows returned by a single query. */
        public const val DEFAULT_MAX_ROWS: Int = 10_000
    }
}

/**
 * Runs [block] inside a database transaction (`BEGIN` ... `COMMIT` / `ROLLBACK`).
 *
 * `NonCancellable` is used strictly for the final `COMMIT` or `ROLLBACK` statement so
 * that transactions never remain dangling when a coroutine fails, while cancellation
 * during [block] rolls back the transaction before closing and discarding the connection.
 *
 * @param T the return type of [block].
 * @param block suspending transactional work receiving the active [DbConnection].
 * @return the result of [block].
 */
@Suppress("TooGenericExceptionCaught")
public suspend fun <T> CoroutinePool.transaction(block: suspend (DbConnection) -> T): T =
    withConnection { conn ->
        conn.execute("BEGIN")
        val result =
            try {
                block(conn)
            } catch (t: Throwable) {
                withContext(NonCancellable) {
                    runCatching { conn.execute("ROLLBACK") }
                }
                throw t
            }
        withContext(NonCancellable) {
            conn.execute("COMMIT")
        }
        result
    }
