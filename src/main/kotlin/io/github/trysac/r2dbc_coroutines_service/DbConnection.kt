package io.github.trysac.r2dbc_coroutines_service

/**
 * Suspending database connection port shaped after the R2DBC SPI.
 */
public interface DbConnection {
    /**
     * Executes a parameterized SQL statement and returns the materialized rows.
     *
     * @param sql the SQL text to execute.
     * @param params positional parameters bound to the statement.
     * @return a list of rows where each row maps column names to values.
     */
    public suspend fun execute(
        sql: String,
        params: List<Any?> = emptyList(),
    ): List<Map<String, Any?>>

    /**
     * Closes the underlying physical connection and releases its resources.
     */
    public suspend fun close()
}

/**
 * Factory that creates new [DbConnection] instances on demand.
 */
public fun interface ConnectionFactory {
    /**
     * Opens and returns a new [DbConnection].
     */
    public suspend fun create(): DbConnection
}
