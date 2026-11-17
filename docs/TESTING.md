# Testing

## Running

```sh
mvn test                  # JUnit 5 and jqwik tests, with a JaCoCo report in target/site/jacoco/
java scripts/Check.java   # every gate, as CI and the factory run it
```

## Policy

- Line coverage of the library (measured by JaCoCo) must stay at or above 80 %; the gate fails
  below it.
- Tests exercise the public API. They assert every observable effect: returned values, thrown
  exceptions (`assertThrows`) and their messages.
- Property tests (jqwik `@Property`) state invariants such as idempotence or round trips.
- Each bug fix starts with a failing test that reproduces it.
- Tests are deterministic: no network, no wall clock, no dependence on test order.

## Tiers

- Local tier (every gate run, coverage measured here): unit and property tests, plus conformance
  on small fixtures kept in the repository.
- CI tier: conformance that needs an external tool or a download. Tag the test class or method
  with `@Tag("conformance")`; Surefire excludes the tag unless `scripts/Check.java` runs with
  `FABRICA_CI=1`, while the lint gate always compiles it. The CI `conformance` job sets
  `FABRICA_CI=1` and runs the full suite. Tools are installed by the optional
  `ci/conformance-setup.sh` (bash, CI only; verify every download against a pinned SHA-256), and
  tests skip cleanly (`Assumptions.assumeTrue`) when their tool is missing.

## Layout

Tests live in `src/test/java/` in the library's package, one `*Test.java` class per area.
Fixtures and golden files go in `src/test/resources/fixtures/`.
