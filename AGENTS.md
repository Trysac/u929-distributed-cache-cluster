# AGENTS.md

Instructions for coding agents (and humans) working on the Maven artifact
`io.github.trysac:distributed-cache-cluster` (Java package `io.github.trysac.distributed_cache_cluster`).

## Commands

| Task | Command |
|---|---|
| Fetch dependencies | `mvn dependency:go-offline` |
| Run every gate (must pass before a PR) | `java scripts/Check.java` |
| Tests only | `mvn test` |
| Format | `mvn spotless:apply` |
| Lint | `mvn test-compile` (javac `-Xlint:all` and Error Prone, warnings are errors) |
| Coverage | `mvn test`, then `target/site/jacoco/index.html` |

The gate sequence is: google-java-format (`mvn spotless:check`), javac `-Xlint:all` plus Error
Prone with `-Werror` and doclint on the public API, Semgrep (the factory's rules in `.semgrep/`),
JUnit 5 and jqwik tests under JaCoCo with line coverage >= 80 % of the library, documentation
manifest (`docs-manifest.json`), gitleaks, OSV-Scanner over every artifact Maven resolves, and
OSV-Scanner over `pom.xml`. It stops at the first failure.

## Folder map

| Path | Content |
|---|---|
| `src/main/java/io/github/trysac/distributed_cache_cluster/` | The library; its public classes are the public API. |
| `src/test/java/io/github/trysac/distributed_cache_cluster/` | JUnit 5 tests (`*Test.java`) and jqwik property tests. |
| `src/test/resources/fixtures/` | Optional fixtures and golden files used by tests. |
| `scripts/` | The single definition of the quality gates (`Check.java`) and the factory's dependency tool. |
| `docs/` | Architecture, decisions (ADRs), testing, security, project record, stack. |
| `fabrica.json` | Install and check commands used by the factory and CI. |
| `docs-manifest.json` | Documentation files the docs gate requires. |
| `.github/` | CI wrapper and Dependabot configuration. |

## Conventions

- Java 21 (`maven.compiler.release`), formatted with google-java-format, clean under javac
  `-Xlint:all` and Error Prone.
- Standard library only unless an ADR in `docs/decisions/` justifies a dependency.
- Every public class and member has a Javadoc comment; invalid input throws a documented
  `IllegalArgumentException` subclass, never returns null or a sentinel value.
- Write the failing test first; tests use every literal of the acceptance items verbatim, and
  jqwik properties (`@Property`) cover invariants.
- CI-only conformance tests (external tools or downloads) carry `@Tag("conformance")`; only CI runs
  them.
- Every behavior change updates `README.md` usage, `CHANGELOG.md` (Unreleased) and, if it is a
  design choice, a new ADR.
- Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`).

## Limits

- Never push to `main`; work on a branch and open a pull request.
- Do NOT edit `.github/`, `.mvn/`, `fabrica.json`, `docs-manifest.json`, `scripts/`, `pom.xml` or
  any tool configuration. They are the quality contract; changes to them are made by a human.
- Never lower the coverage threshold, add `@SuppressWarnings` to silence a lint, or disable a test
  to make a gate pass.
- Never commit secrets, tokens or `.env` files; gitleaks blocks them.
