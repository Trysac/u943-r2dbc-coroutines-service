package io.github.trysac.r2dbc_coroutines_service

/** Text validation and normalization helpers used across the service layer. */
public object Text {
    /**
     * Trims surrounding whitespace and lower-cases [input].
     *
     * @param input the raw string to normalize.
     * @return the trimmed, lower-case string.
     * @throws BlankInputException when nothing is left after trimming.
     */
    public fun normalize(input: String): String {
        val trimmed = input.trim()
        if (trimmed.isEmpty()) {
            throw BlankInputException()
        }
        return trimmed.lowercase()
    }
}
