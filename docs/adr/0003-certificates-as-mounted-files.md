---
status: accepted
---
# TLS key material comes from mounted files, not the database

Connections support `NONE`, `TLS` and `MUTUAL_TLS`. Keystores and truststores are referenced by path and mounted from Kubernetes secrets. Uploading PKCS12 files through the UI into PostgreSQL was rejected: it would make the tool's database a store of private keys. Connection passwords, which are lower risk, are stored in PostgreSQL encrypted with AES-GCM under a key from an environment variable. The app refuses to start without that key.

## Consequences

- One deployment-wide truststore and keystore cover most sites, with an optional per-Connection path override.
- Old queue managers need legacy cipher specs that modern JVMs disable. The image ships a documented `java.security` override.
