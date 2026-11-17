package io.github.trysac.r2dbc_coroutines_service

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Acceptance and edge-case tests for [CoroutinePool], [transaction], [EventLoopGuard],
 * [blocking], and [OrderService].
 */
@Suppress("ThrowingExceptionFromFinally")
@OptIn(ExperimentalCoroutinesApi::class, DelicateCoroutinesApi::class)
class CoroutinePoolAcceptanceTest {
    @AfterEach
    fun cleanupGuard() {
        EventLoopGuard.reset()
    }

    @Test
    fun `100 concurrent withConnection calls complete with at most 4 open connections`() =
        runTest {
            val factory =
                FakeConnectionFactory(
                    createDelay = 5.milliseconds,
                    defaultQueryDelay = 10.milliseconds,
                )
            val pool = CoroutinePool(factory = factory, maxSize = 4)

            val results =
                (1..100)
                    .map { idx ->
                        async {
                            pool.withConnection { conn ->
                                conn.execute("SELECT $1", listOf(idx))
                                idx
                            }
                        }
                    }.awaitAll()

            assertEquals(100, results.size)
            assertEquals((1..100).toList(), results)
            assertTrue(
                factory.maxOpenCount() <= 4,
                "Expected at most 4 open connections, got ${factory.maxOpenCount()}",
            )
            assertEquals(4, factory.maxOpenCount())
            assertEquals(4, pool.idleCount())
            assertEquals(4, pool.createdCount())
        }

    @Test
    fun `5th withConnection when 4 connections held throws PoolException acquire_timeout at virtual time 500`() =
        runTest {
            val factory = FakeConnectionFactory()
            val pool =
                CoroutinePool(
                    factory = factory,
                    maxSize = 4,
                    acquireTimeout = 500.milliseconds,
                )
            val holdGate = CompletableDeferred<Unit>()
            val acquiredSignals = (1..4).map { CompletableDeferred<Unit>() }

            val holders =
                (0 until 4).map { i ->
                    launch {
                        pool.withConnection {
                            acquiredSignals[i].complete(Unit)
                            holdGate.await()
                        }
                    }
                }
            acquiredSignals.awaitAll()
            assertEquals(0L, currentTime)

            val ex =
                assertThrows<PoolException> {
                    pool.withConnection {
                        it.execute("SELECT 1")
                    }
                }
            assertEquals("acquire_timeout", ex.code)
            assertEquals(500L, currentTime)

            holdGate.complete(Unit)
            holders.forEach { it.join() }
        }

    @Test
    fun `cancelling a caller during a query closes that connection instead of returning it to the pool`() =
        runTest {
            val queryEntered = CompletableDeferred<Unit>()
            val factory =
                FakeConnectionFactory { _, sql, _ ->
                    if (sql == "SLOW") {
                        queryEntered.complete(Unit)
                        delay(10.seconds)
                    }
                    emptyList()
                }
            val pool = CoroutinePool(factory = factory, maxSize = 4)
            val holdGate = CompletableDeferred<Unit>()
            val readySignals = (1..3).map { CompletableDeferred<Unit>() }

            val holders =
                (0 until 3).map { idx ->
                    launch {
                        pool.withConnection { conn ->
                            conn.execute("SELECT 1")
                            readySignals[idx].complete(Unit)
                            holdGate.await()
                        }
                    }
                }
            readySignals.awaitAll()

            val statementsAfterCancel = AtomicInteger(0)
            val cancelledJob =
                launch {
                    pool.withConnection { conn ->
                        conn.execute("SLOW")
                        statementsAfterCancel.incrementAndGet()
                        conn.execute("AFTER_CANCEL")
                    }
                }

            queryEntered.await()
            assertEquals(4, pool.createdCount())

            cancelledJob.cancel()
            cancelledJob.join()
            holdGate.complete(Unit)
            holders.forEach { it.join() }

            assertEquals(3, pool.idleCount())
            assertEquals(0, statementsAfterCancel.get())
            assertFalse(factory.statements.contains("AFTER_CANCEL"))

            // Acquire all 4 slots so that the pool must create a 5th connection to replace the closed 4th one
            val roundTwoGate = CompletableDeferred<Unit>()
            val roundTwoReady = (1..4).map { CompletableDeferred<Unit>() }
            val roundTwoJobs =
                (0 until 4).map { idx ->
                    launch {
                        pool.withConnection {
                            roundTwoReady[idx].complete(Unit)
                            roundTwoGate.await()
                        }
                    }
                }
            roundTwoReady.awaitAll()
            assertEquals(5, pool.createdCount())
            roundTwoGate.complete(Unit)
            roundTwoJobs.forEach { it.join() }
        }

    @Test
    fun `transaction sends BEGIN INSERT COMMIT on success and ROLLBACK on failure`() =
        runTest {
            val factory = FakeConnectionFactory()
            val pool = CoroutinePool(factory = factory, maxSize = 2)

            pool.transaction { conn ->
                conn.execute("INSERT INTO orders VALUES ($1, $2)", listOf("cust-1", 1500L))
            }

            assertEquals(
                listOf("BEGIN", "INSERT INTO orders VALUES ($1, $2)", "COMMIT"),
                factory.statements,
            )

            factory.statements.clear()
            val boom = IllegalStateException("domain failure")
            val thrown =
                assertThrows<IllegalStateException> {
                    pool.transaction { conn ->
                        conn.execute("INSERT INTO orders VALUES ($1, $2)", listOf("cust-1", 2500L))
                        throw boom
                    }
                }
            assertSame(boom, thrown)
            assertEquals(
                listOf("BEGIN", "INSERT INTO orders VALUES ($1, $2)", "ROLLBACK"),
                factory.statements,
            )
        }

    @Test
    fun `EventLoopGuard blocks on event loop thread and OrderService records 0 violations`() {
        val violations = CopyOnWriteArrayList<String>()
        EventLoopGuard.onViolation = { threadName -> violations.add(threadName) }

        newSingleThreadContext("event-loop-0").use { eventLoopDispatcher ->
            runBlocking(eventLoopDispatcher) {
                EventLoopGuard.register(Thread.currentThread())
                assertTrue(EventLoopGuard.isEventLoop())

                val ex =
                    assertThrows<ServiceException> {
                        blocking<Int> { 42 }
                    }
                assertEquals("blocking_on_event_loop", ex.code)
                assertEquals(1, violations.size)
            }

            // Same call from a non-event-loop thread succeeds on the dedicated blocking dispatcher
            runBlocking {
                assertFalse(EventLoopGuard.isEventLoop())
                val value =
                    blocking {
                        assertFalse(EventLoopGuard.isEventLoop())
                        "offloaded"
                    }
                assertEquals("offloaded", value)
            }

            // Reset violation log and run 1_000 OrderService calls on the single-thread event-loop dispatcher
            violations.clear()
            runBlocking(eventLoopDispatcher) {
                val factory = FakeConnectionFactory()
                val pool = CoroutinePool(factory = factory, maxSize = 4)
                val service = OrderService(pool)

                repeat(1_000) { idx ->
                    val id = service.create("Customer-$idx", (idx + 1) * 100L)
                    assertTrue(id >= 1L)
                }
                val orders = service.listByCustomer("Customer-0")
                assertEquals(1, orders.size)
                assertEquals("customer-0", orders.first().customerId)
                assertEquals(100L, orders.first().amountCents)
                pool.close()
            }
            assertEquals(0, violations.size)
        }
    }

    @Test
    fun `bounds enforce sql_too_long, too_many_rows, and query_timeout at 2s virtual time`() =
        runTest {
            val factory =
                FakeConnectionFactory { _, sql, _ ->
                    when (sql) {
                        "MANY_ROWS" -> {
                            List(10_001) { mapOf("idx" to it) }
                        }

                        "SLOW_QUERY" -> {
                            delay(5.seconds)
                            emptyList()
                        }

                        else -> {
                            emptyList()
                        }
                    }
                }
            val pool =
                CoroutinePool(
                    factory = factory,
                    maxSize = 2,
                    queryTimeout = 2.seconds,
                    maxRows = 10_000,
                )

            // 1. SQL of 65_537 characters gives 'sql_too_long'
            val longSql = "S".repeat(65_537)
            val sqlEx =
                assertThrows<ServiceException> {
                    pool.withConnection { conn ->
                        conn.execute(longSql)
                    }
                }
            assertEquals("sql_too_long", sqlEx.code)
            // Validation failure is a clean ServiceException, so connection remains reusable
            assertEquals(1, pool.idleCount())

            // 2. Result above maxRows 10_000 gives 'too_many_rows'
            val rowsEx =
                assertThrows<ServiceException> {
                    pool.withConnection { conn ->
                        conn.execute("MANY_ROWS")
                    }
                }
            assertEquals("too_many_rows", rowsEx.code)
            assertEquals(1, pool.idleCount())

            // 3. Query exceeding queryTimeout 2 s of virtual time gives 'query_timeout' and closes the connection
            val startVirtualTime = currentTime
            val timeoutEx =
                assertThrows<ServiceException> {
                    pool.withConnection { conn ->
                        conn.execute("SLOW_QUERY")
                    }
                }
            assertEquals("query_timeout", timeoutEx.code)
            assertEquals(2_000L, currentTime - startVirtualTime)
            assertEquals(0, pool.idleCount())
            assertTrue(factory.connections.first().closed)
        }

    @Test
    fun `edge cases - nested withConnection on same coroutine acquires a second connection`() =
        runTest {
            val factory = FakeConnectionFactory()
            val pool = CoroutinePool(factory = factory, maxSize = 2)

            pool.withConnection { outer ->
                outer.execute("SELECT 1")
                pool.withConnection { inner ->
                    inner.execute("SELECT 2")
                    assertEquals(2, factory.openCount())
                }
            }

            assertEquals(2, pool.idleCount())
            assertEquals(2, pool.createdCount())
        }

    @Test
    fun `edge cases - exception inside transaction after cancellation still executes ROLLBACK and closes connection`() =
        runTest {
            val insideTx = CompletableDeferred<Unit>()
            val factory = FakeConnectionFactory()
            val pool = CoroutinePool(factory = factory, maxSize = 1)
            val surfaced = CompletableDeferred<Throwable>()

            // The block's own exception must reach the caller unchanged; it is caught inside the
            // child so it does not fail the test scope through structured concurrency.
            val job =
                launch {
                    try {
                        pool.transaction { conn ->
                            conn.execute("INSERT INTO orders VALUES ($1, $2)", listOf("alice", 500L))
                            insideTx.complete(Unit)
                            try {
                                delay(10.seconds)
                            } finally {
                                error("exception after cancellation")
                            }
                        }
                    } catch (e: IllegalStateException) {
                        surfaced.complete(e)
                    }
                }

            insideTx.await()
            job.cancel()
            job.join()

            assertEquals("exception after cancellation", surfaced.await().message)
            assertEquals(
                listOf("BEGIN", "INSERT INTO orders VALUES ($1, $2)", "ROLLBACK"),
                factory.statements,
            )
            assertEquals(0, pool.idleCount())
            assertTrue(factory.connections.single().closed)
        }

    @Test
    fun `edge cases - maxSize 1 serves waiting callers strictly FIFO`() =
        runTest {
            val factory = FakeConnectionFactory()
            val pool = CoroutinePool(factory = factory, maxSize = 1)
            val firstGate = CompletableDeferred<Unit>()
            val firstAcquired = CompletableDeferred<Unit>()
            val order = CopyOnWriteArrayList<Int>()

            val holder =
                launch {
                    pool.withConnection {
                        firstAcquired.complete(Unit)
                        firstGate.await()
                        order.add(1)
                    }
                }
            firstAcquired.await()

            val waiters =
                (2..5).map { id ->
                    launch {
                        pool.withConnection {
                            order.add(id)
                        }
                    }
                }
            // Yield virtual time so all waiters enqueue in order
            delay(10.milliseconds)
            firstGate.complete(Unit)
            holder.join()
            waiters.forEach { it.join() }

            assertEquals(listOf(1, 2, 3, 4, 5), order)
            assertEquals(1, pool.createdCount())
            assertEquals(1, pool.idleCount())
        }

    @Test
    fun `closing pool fails waiting and future acquires with pool_closed and closes idle connections`() =
        runTest {
            val factory = FakeConnectionFactory()
            val pool = CoroutinePool(factory = factory, maxSize = 2)

            pool.withConnection { it.execute("SELECT 1") }
            assertEquals(1, pool.idleCount())

            pool.close()
            assertEquals(0, pool.idleCount())
            assertTrue(factory.connections.first().closed)

            val ex =
                assertThrows<ServiceException> {
                    pool.withConnection { it.execute("SELECT 1") }
                }
            assertEquals("pool_closed", ex.code)
        }

    @Test
    fun `factory failure releases reserved slot so subsequent callers can acquire`() =
        runTest {
            var shouldFail = true
            val factory =
                ConnectionFactory {
                    if (shouldFail) {
                        error("connect failed")
                    }
                    FakeConnectionFactory().create()
                }
            val pool = CoroutinePool(factory = factory, maxSize = 1)

            assertThrows<IllegalStateException> {
                pool.withConnection { it.execute("SELECT 1") }
            }

            shouldFail = false
            val rows =
                pool.withConnection { conn ->
                    conn.execute("SELECT 1")
                }
            assertTrue(rows.isEmpty())
            assertEquals(1, pool.idleCount())
        }

    @Test
    fun `invalid pool configuration parameters and OrderService validation throw expected errors`() =
        runTest {
            val factory = FakeConnectionFactory()
            assertThrows<IllegalArgumentException> {
                CoroutinePool(factory, maxSize = 0)
            }
            assertThrows<IllegalArgumentException> {
                CoroutinePool(factory, acquireTimeout = 0.seconds)
            }
            assertThrows<IllegalArgumentException> {
                CoroutinePool(factory, queryTimeout = 0.seconds)
            }
            assertThrows<IllegalArgumentException> {
                CoroutinePool(factory, maxRows = 0)
            }

            val pool = CoroutinePool(factory)
            val service = OrderService(pool)
            assertThrows<BlankInputException> {
                service.create("   ", 100L)
            }
            val negEx =
                assertThrows<ServiceException> {
                    service.create("alice", -1L)
                }
            assertEquals("invalid_argument", negEx.code)

            val t = Thread.currentThread()
            EventLoopGuard.register(t)
            assertTrue(EventLoopGuard.isEventLoop())
            EventLoopGuard.unregister(t)
            assertFalse(EventLoopGuard.isEventLoop())

            val longMsgEx =
                withContext(coroutineContext) {
                    ServiceException("custom", "x".repeat(250))
                }
            assertEquals(200, longMsgEx.message?.length)
        }
}
