package io.github.trysac.r2dbc_coroutines_service

/** The input is empty or holds only whitespace; the message is `input is blank`. */
public class BlankInputException : IllegalArgumentException("input is blank")
