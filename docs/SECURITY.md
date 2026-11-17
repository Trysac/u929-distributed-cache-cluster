# Security notes

## Threat model

`io.github.trysac:distributed-cache-cluster` is a pure library without I/O. The main risks are:

- **Supply chain**: a compromised dependency, Maven plugin or CI action. Mitigation: standard
  library only, every plugin and dependency pinned to an exact version in `pom.xml`, OSV-Scanner
  over every artifact Maven resolves and over `pom.xml` in the gates, Dependabot weekly updates,
  actions pinned by commit SHA, minimal workflow permissions.
- **Leaked secrets**: tokens committed by mistake. Mitigation: `gitleaks` runs in the local gates
  and in CI; `.env` files are git-ignored.
- **Insecure code**: command injection, unsafe deserialization, disabled TLS verification.
  Mitigation: Error Prone and javac `-Xlint:all` with warnings as errors, and the factory's Semgrep
  rules (`.semgrep/factory-rules.yml`, `nosemgrep` ignored); fix the code instead of silencing it.
- **Untrusted input**: callers may pass hostile strings. Mitigation: validate input and throw
  explicit, documented exceptions; never deserialize untrusted bytes with `ObjectInputStream`;
  never start a shell with untrusted data.

## Publishing

Releases are git tags; publishing to Maven Central needs a namespace account and a signing key,
which are never stored in the repository.

To report a vulnerability, see [SECURITY.md](../SECURITY.md).
