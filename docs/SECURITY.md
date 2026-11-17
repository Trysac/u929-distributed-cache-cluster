# Security notes

## Threat model

`io.github.trysac:distributed-cache-cluster` is a library without I/O and does not perform network operations on its own. 

### Defended

- **Supply chain attacks**: Standard library only, dependencies pinned.
- **Secrets leaking**: `gitleaks` prevents commits.
- **Input validation**: `CacheNode` explicitly rejects blank IDs and input exceeding limits (`key > 1024 bytes`, `value > 1048576 bytes`).

### Not defended

- **Untrusted data deserialization**: Callers are responsible for the contents of `byte[]` arrays passed to `put`. Never pass untrusted, serialized objects directly without validation.
- **System security**: The simulator is designed for testing and assumes the host environment is trusted.

To report a vulnerability, please check the repository metadata or contact the maintainer.