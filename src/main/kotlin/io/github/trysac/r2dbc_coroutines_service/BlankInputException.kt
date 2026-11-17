package io.github.trysac.r2dbc_coroutines_service

/**
 * Thrown when a required text argument is empty or contains only whitespace.
 *
 * @param message the detail message explaining the validation failure.
 */
public class BlankInputException(
    message: String = "input is blank",
) : IllegalArgumentException(message)
