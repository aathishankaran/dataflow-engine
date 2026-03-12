# vault

HashiCorp Vault credential reader for secure Oracle database access. Supports token-based and AppRole authentication with KV v2/v1 fallback.

---

## Overview

The `vault` module provides secure credential management:

- **Token authentication**: Uses `VAULT_TOKEN` environment variable
- **AppRole authentication**: Uses `VAULT_ROLE_ID` and `VAULT_SECRET_ID` environment variables
- **KV version fallback**: Tries KV v2 first, falls back to KV v1 if not available

Credentials are never stored in configuration files. They are fetched from Vault at runtime when an `oracle_write` transformation step is executed.

See [Design Decisions](../design-decisions.md) for the rationale behind runtime credential fetching.

---

## API Documentation

::: dataflow_engine.vault
    options:
      show_root_heading: false
      members_order: source
      filters: []
