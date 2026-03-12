"""
HashiCorp Vault credential reader for the Dataflow Engine.

Reads Oracle (and other) database credentials from a Vault KV secret at
runtime so that passwords are *never* stored inside a dataflow config JSON.

Authentication (picked in order):
  1. VAULT_TOKEN   environment variable  (simplest — dev / direct token)
  2. VAULT_ROLE_ID + VAULT_SECRET_ID     (AppRole — recommended for production)

Required environment variable:
  VAULT_ADDR  — e.g.  https://vault.example.com:8200  (default: http://127.0.0.1:8200)

Optional environment variables:
  VAULT_NAMESPACE — Vault Enterprise namespace (omit for open-source Vault)
  VAULT_SKIP_VERIFY — set to "true" to disable TLS certificate verification

Secret format (KV v2 example):
  vault kv put secret/oracle/prod username="svc_acct" password="s3cr3t"
"""

import logging
import os
from typing import Optional, Tuple

LOG = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def get_oracle_credentials(
    vault_path: str,
    username_key: str = "username",
    password_key: str = "password",
) -> Tuple[str, str]:
    """Read Oracle credentials from HashiCorp Vault.

    Parameters
    ----------
    vault_path   : Vault KV path, e.g. ``secret/data/oracle/prod`` (KV v2)
                   or ``secret/oracle/prod`` (KV v1 / auto-detected).
    username_key : Key name in the secret whose value is the DB username.
    password_key : Key name in the secret whose value is the DB password.

    Returns
    -------
    (username, password) — both are non-empty strings.

    Raises
    ------
    RuntimeError : if hvac is missing, auth fails, or the secret is not found.
    """
    client = _build_vault_client()
    data = _read_secret(client, vault_path)

    username = data.get(username_key)
    password = data.get(password_key)

    if not username:
        raise RuntimeError(
            f"Vault secret at '{vault_path}' does not contain key '{username_key}'. "
            f"Available keys: {list(data.keys())}"
        )
    if password is None:
        raise RuntimeError(
            f"Vault secret at '{vault_path}' does not contain key '{password_key}'. "
            f"Available keys: {list(data.keys())}"
        )

    LOG.info("Vault: retrieved Oracle credentials for user '%s' from '%s'", username, vault_path)
    return str(username), str(password)


def store_oracle_credentials(
    vault_path: str,
    username: str,
    password: str,
    extra_fields: Optional[dict] = None,
) -> None:
    """Write (or update) Oracle credentials in Vault KV v2.

    This is a convenience helper for initial secret provisioning; it is NOT
    called during normal pipeline execution.

    Parameters
    ----------
    vault_path   : KV v2 path, e.g. ``secret/data/oracle/prod``.
    username     : Oracle DB username to store.
    password     : Oracle DB password to store.
    extra_fields : Optional additional key/value pairs to store alongside
                   the credentials (e.g. ``{"host": "...", "port": "1521"}``).

    Raises
    ------
    RuntimeError : if hvac is missing, auth fails, or the write fails.
    """
    client = _build_vault_client()

    secret_data: dict = {"username": username, "password": password}
    if extra_fields:
        secret_data.update(extra_fields)

    mount, path = _split_kv_path(vault_path)
    try:
        client.secrets.kv.v2.create_or_update_secret(
            path=path,
            secret=secret_data,
            mount_point=mount,
        )
        LOG.info("Vault: stored Oracle credentials for user '%s' at '%s'", username, vault_path)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to write secret to Vault at '{vault_path}': {exc}"
        ) from exc


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_vault_client():
    """Create and authenticate an hvac.Client from environment variables."""
    try:
        import hvac  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "The 'hvac' library is required for HashiCorp Vault integration. "
            "Install it with:  pip install hvac"
        ) from exc

    vault_addr      = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
    vault_token     = os.environ.get("VAULT_TOKEN")
    vault_role_id   = os.environ.get("VAULT_ROLE_ID")
    vault_secret_id = os.environ.get("VAULT_SECRET_ID")
    vault_namespace = os.environ.get("VAULT_NAMESPACE")
    skip_verify     = os.environ.get("VAULT_SKIP_VERIFY", "").lower() in ("1", "true", "yes")

    LOG.debug("Vault: connecting to %s (namespace=%s, skip_verify=%s)", vault_addr, vault_namespace, skip_verify)

    client = hvac.Client(
        url=vault_addr,
        namespace=vault_namespace or None,
        verify=not skip_verify,
    )

    if vault_token:
        client.token = vault_token
        LOG.debug("Vault: authenticating with VAULT_TOKEN")
    elif vault_role_id and vault_secret_id:
        LOG.debug("Vault: authenticating via AppRole")
        try:
            client.auth.approle.login(
                role_id=vault_role_id,
                secret_id=vault_secret_id,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Vault AppRole login failed: {exc}"
            ) from exc
    else:
        raise RuntimeError(
            "Vault authentication not configured. "
            "Set VAULT_TOKEN  —or—  VAULT_ROLE_ID + VAULT_SECRET_ID  environment variables."
        )

    if not client.is_authenticated():
        raise RuntimeError(
            "Vault client is not authenticated after login. "
            "Check your token / AppRole credentials and VAULT_ADDR."
        )

    LOG.debug("Vault: authenticated successfully")
    return client


def _split_kv_path(vault_path: str) -> Tuple[str, str]:
    """Split a Vault KV path into (mount_point, secret_path).

    Examples
    --------
    ``secret/data/oracle/prod``  →  (``secret``, ``oracle/prod``)   KV v2
    ``secret/oracle/prod``       →  (``secret``, ``oracle/prod``)   KV v1
    ``kv/data/mydb``             →  (``kv``,     ``mydb``)
    """
    parts = vault_path.lstrip("/").split("/")
    mount = parts[0]
    # Strip the 'data' segment that KV v2 requires in the HTTP path but hvac
    # inserts automatically when calling read_secret_version().
    rest  = parts[1:]
    if rest and rest[0] == "data":
        rest = rest[1:]
    path = "/".join(rest)
    return mount, path


def _read_secret(client, vault_path: str) -> dict:
    """Read secret data from Vault, trying KV v2 then KV v1 as fallback."""
    mount, path = _split_kv_path(vault_path)

    LOG.debug("Vault: reading secret mount='%s' path='%s'", mount, path)

    # ── Try KV v2 first ─────────────────────────────────────────────────────
    try:
        response = client.secrets.kv.v2.read_secret_version(
            path=path,
            mount_point=mount,
            raise_on_deleted_version=True,
        )
        data: dict = response["data"]["data"]
        LOG.debug("Vault: KV v2 read succeeded for path '%s'", vault_path)
        return data
    except Exception as kv2_err:
        LOG.debug("Vault: KV v2 read failed (%s), trying KV v1", kv2_err)

    # ── Fall back to KV v1 ──────────────────────────────────────────────────
    try:
        response = client.secrets.kv.v1.read_secret(
            path=path,
            mount_point=mount,
        )
        data = response["data"]
        LOG.debug("Vault: KV v1 read succeeded for path '%s'", vault_path)
        return data
    except Exception as kv1_err:
        raise RuntimeError(
            f"Failed to read secret from Vault path '{vault_path}'. "
            f"KV v2 error: {kv2_err}  |  KV v1 error: {kv1_err}"
        ) from kv1_err
