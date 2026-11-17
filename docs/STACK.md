# Stack

## Used

| Tool | Why |
|---|---|
| Java 21 (LTS) | `maven.compiler.release` 21; records, pattern matching. |
| Maven | Standard build; plugins pinned in `pom.xml`. |
| google-java-format | Canonical code layout. |
| javac `-Xlint:all` + Error Prone | Compile-time linting and bug prevention. |
| JUnit 5 + jqwik | Property and unit testing. |
| JaCoCo | Coverage measurement. |
| Semgrep | Factory-standard security rules. |

## Discarded

| Tool | Why not |
|---|---|
| Gradle | Maven's `pom.xml` is preferred for factory consistency. |
| Checkstyle | google-java-format handles formatting. |
| OWASP | OSV-Scanner handles vulnerability scanning for Maven artifacts. |