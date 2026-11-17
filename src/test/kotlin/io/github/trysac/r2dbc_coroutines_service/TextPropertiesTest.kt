package io.github.trysac.r2dbc_coroutines_service

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import net.jqwik.api.ForAll
import net.jqwik.api.Property
import net.jqwik.api.constraints.AlphaChars
import net.jqwik.api.constraints.IntRange
import net.jqwik.api.constraints.StringLength
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue

/** Property-based tests for text normalization and connection pool invariants. */
class TextPropertiesTest {
    @Property
    fun normalizedTextIsTrimmedAndLowercase(
        @ForAll @AlphaChars @StringLength(min = 1, max = 40) raw: String,
    ) {
        val padded = "  $raw\n"
        val normalized = Text.normalize(padded)
        assertEquals(normalized.trim(), normalized)
        assertEquals(normalized.lowercase(), normalized)
    }

    @Property(tries = 30)
    fun poolNeverExceedsMaxSizeUnderConcurrentLoad(
        @ForAll @IntRange(min = 1, max = 8) maxSize: Int,
        @ForAll @IntRange(min = 1, max = 40) callers: Int,
    ) {
        runTest {
            val factory = FakeConnectionFactory()
            val pool = CoroutinePool(factory = factory, maxSize = maxSize)
            (1..callers)
                .map { idx ->
                    async {
                        pool.withConnection { conn ->
                            conn.execute("SELECT $1", listOf(idx))
                        }
                    }
                }.awaitAll()
            assertTrue(factory.maxOpenCount() <= maxSize)
            assertTrue(pool.idleCount() <= maxSize)
            pool.close()
        }
    }
}
