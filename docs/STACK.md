# Stack

## Used

| Tool | Why |
|---|---|
| Java 21 (LTS) | `maven.compiler.release` 21; records, pattern matching and the single-file launcher for scripts. |
| Maven | The standard Java build; every plugin pinned to an exact version in `pom.xml`. |
| google-java-format (Spotless) | One canonical layout, no options to argue about; Spotless runs it from Maven. |
| javac `-Xlint:all` + Error Prone | Compiler warnings and Error Prone's bug patterns, both as errors; doclint on the public API. |
| JUnit 5 + jqwik | Example-based and property-based tests on the JUnit Platform. |
| JaCoCo | Line coverage of the library; the gate reads its CSV report. |
| Semgrep CE + OSV-Scanner | The factory's own rules; OSV advisories over Maven's resolved artifacts and over `pom.xml`. |
| gitleaks | Secret scanning locally and in CI. |
| GitHub Actions + Dependabot | Thin CI wrapper over the local gates; weekly dependency updates. |

## Discarded

| Tool | Why not |
|---|---|
| Gradle | Maven's declarative `pom.xml` is simpler to keep factory-owned; Gradle builds are code. |
| Checkstyle / SpotBugs | google-java-format fixes layout and Error Prone covers the bug patterns at compile time. |
| OWASP dependency-check | Needs the NVD database (a large download with an API key); OSV covers Maven advisories. |
| Shell scripts for gates | Not portable to Windows; the gates are a Java program in `scripts/Check.java`. |
