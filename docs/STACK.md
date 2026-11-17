# Stack

## Dependencies

- **Kotlin 2 (JVM 21)**: Targeting JVM 21 bytecode.
- **kotlinx-coroutines-core**: Core concurrency primitives (Mutex, Channel, withTimeout).
- **kotlinx-coroutines-test**: Testing infrastructure for virtual time and structured concurrency.

## Tooling

- **Gradle (Kotlin DSL)**: Build system with locked dependencies.
- **JUnit 5 + jqwik**: Used for example-based and property-based testing.
- **Kover**: Line coverage enforcement (gate threshold: 80%).
- **Detekt**: Enforces KDoc on public API and clean code standards.
