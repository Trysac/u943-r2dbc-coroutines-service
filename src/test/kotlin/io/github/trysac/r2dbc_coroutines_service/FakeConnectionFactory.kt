package io.github.trysac.r2dbc_coroutines_service

import kotlinx.coroutines.delay
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max
import kotlin.time.Duration

/**
 * Recorded SQL execution entry captured by [FakeConnectionFactory].
 */
class RecordedStatement(
    val connectionId: Int,
    val sql: String,
    val params: List<Any?>,
)

/**
 * Deterministic in-memory [ConnectionFactory] for testing pool bounds, virtual-time
 * timeouts, cancellation behavior, and transaction sequences.
 */
class FakeConnectionFactory(
    private val createDelay: Duration = Duration.ZERO,
    private val defaultQueryDelay: Duration = Duration.ZERO,
    private val failOnCreate: Throwable? = null,
    private val queryHandler: (suspend (Int, String, List<Any?>) -> List<Map<String, Any?>>)? = null,
) : ConnectionFactory {
    private val nextConnectionId = AtomicInteger(0)
    private val currentOpen = AtomicInteger(0)
    private val peakOpen = AtomicInteger(0)
    private val nextOrderId = AtomicLong(0L)
    private val storedOrders = ConcurrentHashMap<String, MutableList<Order>>()

    val statements: MutableList<String> = CopyOnWriteArrayList()
    val recordedStatements: MutableList<RecordedStatement> = CopyOnWriteArrayList()
    val connections: MutableList<FakeDbConnection> = CopyOnWriteArrayList()

    fun openCount(): Int = currentOpen.get()

    fun maxOpenCount(): Int = peakOpen.get()

    override suspend fun create(): DbConnection {
        if (createDelay.isPositive()) {
            delay(createDelay)
        }
        failOnCreate?.let { throw it }
        val id = nextConnectionId.incrementAndGet()
        val nowOpen = currentOpen.incrementAndGet()
        peakOpen.updateAndGet { prev -> max(prev, nowOpen) }
        val conn = FakeDbConnection(id)
        connections.add(conn)
        return conn
    }

    inner class FakeDbConnection(
        val id: Int,
    ) : DbConnection {
        var closed: Boolean = false
            private set

        val localStatements: MutableList<String> = CopyOnWriteArrayList()
        private val stagedOrders: MutableList<Order> = Collections.synchronizedList(mutableListOf())
        private var inTransaction: Boolean = false

        override suspend fun execute(
            sql: String,
            params: List<Any?>,
        ): List<Map<String, Any?>> {
            check(!closed) { "Connection #$id is already closed" }
            if (defaultQueryDelay.isPositive()) {
                delay(defaultQueryDelay)
            }
            statements.add(sql)
            localStatements.add(sql)
            recordedStatements.add(RecordedStatement(id, sql, params))
            if (queryHandler != null) {
                return queryHandler.invoke(id, sql, params)
            }
            return handlePredefinedQuery(sql, params)
        }

        private fun handlePredefinedQuery(
            sql: String,
            params: List<Any?>,
        ): List<Map<String, Any?>> =
            when {
                sql == "BEGIN" -> handleBegin()
                sql == "COMMIT" -> handleCommit()
                sql == "ROLLBACK" -> handleRollback()
                sql.startsWith("INSERT INTO orders") -> handleInsert(params)
                sql.startsWith("SELECT") -> handleSelect(params)
                else -> emptyList()
            }

        private fun handleBegin(): List<Map<String, Any?>> {
            inTransaction = true
            stagedOrders.clear()
            return emptyList()
        }

        private fun handleCommit(): List<Map<String, Any?>> {
            for (order in stagedOrders) {
                storedOrders
                    .computeIfAbsent(order.customerId) { CopyOnWriteArrayList() }
                    .add(order)
            }
            stagedOrders.clear()
            inTransaction = false
            return emptyList()
        }

        private fun handleRollback(): List<Map<String, Any?>> {
            stagedOrders.clear()
            inTransaction = false
            return emptyList()
        }

        private fun handleInsert(params: List<Any?>): List<Map<String, Any?>> {
            val customerId = params.getOrNull(0)?.toString() ?: "unknown"
            val amountCents = (params.getOrNull(1) as? Number)?.toLong() ?: 0L
            val orderId = nextOrderId.incrementAndGet()
            val order = Order(id = orderId, customerId = customerId, amountCents = amountCents)
            if (inTransaction) {
                stagedOrders.add(order)
            } else {
                storedOrders
                    .computeIfAbsent(customerId) { CopyOnWriteArrayList() }
                    .add(order)
            }
            return listOf(mapOf("id" to orderId))
        }

        private fun handleSelect(params: List<Any?>): List<Map<String, Any?>> {
            val customerId = params.getOrNull(0)?.toString() ?: ""
            val list = storedOrders[customerId].orEmpty()
            return list.map {
                mapOf(
                    "id" to it.id,
                    "customer_id" to it.customerId,
                    "amount_cents" to it.amountCents,
                )
            }
        }

        override suspend fun close() {
            if (!closed) {
                closed = true
                currentOpen.decrementAndGet()
            }
        }
    }
}
