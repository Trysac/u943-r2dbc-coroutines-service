package io.github.trysac.r2dbc_coroutines_service

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.Volatile

/**
 * Registry and runtime detector for non-blocking event-loop threads.
 *
 * Register event-loop threads via [register] so that accidental calls to [blocking]
 * on an event-loop thread fail immediately and deterministically with a
 * [ServiceException] (`blocking_on_event_loop`).
 */
public object EventLoopGuard {
    private const val MAX_REGISTERED_THREADS: Int = 4096
    private val registeredThreads: MutableSet<Thread> = ConcurrentHashMap.newKeySet()

    /**
     * Optional callback invoked with the violating thread name whenever a blocking
     * operation is attempted on a registered event-loop thread.
     */
    @Volatile
    public var onViolation: ((String) -> Unit)? = null

    /**
     * Registers [thread] as a non-blocking event-loop thread.
     *
     * If the registry reaches its internal capacity bound, the oldest arbitrary entry
     * is evicted before inserting [thread].
     *
     * @param thread the thread to mark as an event loop.
     */
    public fun register(thread: Thread) {
        if (registeredThreads.size >= MAX_REGISTERED_THREADS && !registeredThreads.contains(thread)) {
            val first = registeredThreads.iterator()
            if (first.hasNext()) {
                registeredThreads.remove(first.next())
            }
        }
        registeredThreads.add(thread)
    }

    /**
     * Unregisters [thread] so it is no longer treated as an event-loop thread.
     *
     * @param thread the thread to remove from the guard.
     */
    public fun unregister(thread: Thread) {
        registeredThreads.remove(thread)
    }

    /**
     * Clears all registered threads and resets [onViolation].
     */
    public fun reset() {
        registeredThreads.clear()
        onViolation = null
    }

    /**
     * Returns `true` if the calling thread is currently registered as an event-loop thread.
     */
    public fun isEventLoop(): Boolean = registeredThreads.contains(Thread.currentThread())

    internal fun checkNotEventLoop() {
        val current = Thread.currentThread()
        if (registeredThreads.contains(current)) {
            onViolation?.invoke(current.name)
            throw ServiceException(
                code = "blocking_on_event_loop",
                message = "Blocking call forbidden on event-loop thread: ${current.name}",
            )
        }
    }
}

private const val BLOCKING_PARALLELISM: Int = 16
private val BLOCKING_DISPATCHER: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(BLOCKING_PARALLELISM)

/**
 * Runs a potentially blocking [block] safely off the caller's thread, unless called
 * directly from a thread registered with [EventLoopGuard].
 *
 * When invoked on a registered event-loop thread, this function immediately notifies
 * [EventLoopGuard.onViolation] (if set) and throws [ServiceException] with code
 * `blocking_on_event_loop` before dispatching. Otherwise, it executes [block] on
 * `Dispatchers.IO.limitedParallelism(16)`.
 *
 * @param T the return type of [block].
 * @param block the synchronous operation to execute.
 * @return the result of [block].
 * @throws ServiceException with code `blocking_on_event_loop` if called on an event-loop thread.
 */
public suspend fun <T> blocking(block: () -> T): T {
    EventLoopGuard.checkNotEventLoop()
    return withContext(BLOCKING_DISPATCHER) {
        EventLoopGuard.checkNotEventLoop()
        block()
    }
}
