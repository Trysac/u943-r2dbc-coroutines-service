package io.github.trysac.r2dbc_coroutines_service

import java.util.concurrent.atomic.AtomicLong

/**
 * Immutable domain record representing a customer order.
 *
 * @property id unique numeric identifier of the order.
 * @property customerId normalized identifier of the customer who placed the order.
 * @property amountCents positive order total in cents.
 */
public data class Order(
    public val id: Long,
    public val customerId: String,
    public val amountCents: Long,
)

/**
 * Reference coroutine service backed by [CoroutinePool] and transactional queries.
 *
 * Demonstrates safe non-blocking execution suitable for event-loop dispatchers.
 *
 * @param pool the [CoroutinePool] managing database connections.
 */
public class OrderService(
    private val pool: CoroutinePool,
) {
    private val sequence: AtomicLong = AtomicLong(0L)

    /**
     * Creates a new order for [customerId] with [amountCents] inside a transaction
     * and returns its generated order ID.
     *
     * @param customerId non-blank customer identifier.
     * @param amountCents non-negative amount in cents.
     * @return the newly created order identifier.
     * @throws BlankInputException if [customerId] is blank.
     * @throws ServiceException if [amountCents] is negative or a database error occurs.
     */
    public suspend fun create(
        customerId: String,
        amountCents: Long,
    ): Long {
        val normalizedCustomer = Text.normalize(customerId)
        if (amountCents < 0L) {
            throw ServiceException("invalid_argument", "amountCents must be non-negative")
        }
        return pool.transaction { conn ->
            val rows =
                conn.execute(
                    "INSERT INTO orders VALUES ($1, $2)",
                    listOf(normalizedCustomer, amountCents),
                )
            val returnedId = (rows.firstOrNull()?.get("id") as? Number)?.toLong()
            returnedId ?: sequence.incrementAndGet()
        }
    }

    /**
     * Lists all orders belonging to [customerId].
     *
     * @param customerId non-blank customer identifier.
     * @return the list of matching [Order] instances.
     * @throws BlankInputException if [customerId] is blank.
     */
    public suspend fun listByCustomer(customerId: String): List<Order> {
        val normalizedCustomer = Text.normalize(customerId)
        return pool.withConnection { conn ->
            val rows =
                conn.execute(
                    "SELECT id, customer_id, amount_cents FROM orders WHERE customer_id = $1",
                    listOf(normalizedCustomer),
                )
            rows.mapIndexed { index, row ->
                val id = (row["id"] as? Number)?.toLong() ?: (index + 1).toLong()
                val cust = (row["customer_id"] as? String) ?: normalizedCustomer
                val amount = (row["amount_cents"] as? Number)?.toLong() ?: 0L
                Order(id = id, customerId = cust, amountCents = amount)
            }
        }
    }
}
