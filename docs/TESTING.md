# Testing

## Existing Tests

- **`CoroutinePoolAcceptanceTest.kt`**: Tests pool bounds, FIFO acquisition, cancellation cleanup, transaction semantics, EventLoopGuard behavior, and service-layer validation.
- **`TextPropertiesTest.kt`**: Property-based tests using `jqwik` verifying normalization logic and concurrency invariants.
- **`TextTest.kt`**: Unit tests for `Text` normalization logic and error handling.
- **`FakeConnectionFactory.kt`**: A deterministic in-memory `ConnectionFactory` used by the test suite to verify statement sequencing, simulate failures, and inject delays.

## Policy

- Coverage must stay at or above 80%.
- Tests are deterministic, utilizing virtual time via `runTest`.
- All observable effects, including thrown `ServiceException` codes, are verified.
