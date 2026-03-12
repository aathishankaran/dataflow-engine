"""
incident.py — Moogsoft → ServiceNow Incident Connector
=======================================================

Creates ServiceNow incidents via the Moogsoft AIOps HTTP event endpoint.
Moogsoft processes the inbound event, enriches it with topology context, and
raises a ServiceNow incident through its pre-configured ITSM integration.

Typical usage
-------------
::

    from dataflow_engine.incident import MoogsoftIncidentConnector

    connector = MoogsoftIncidentConnector(
        endpoint="https://your-moogsoft.example.com/events/http",
        api_key="YOUR_MOOGSOFT_API_KEY",
    )

    incident_id = connector.create_incident(
        summary="Data validation failed in dataflow: payment_pipeline",
        description="14 invalid rows detected in step validate_payments.",
        severity="CRITICAL",           # CRITICAL | HIGH | MEDIUM | LOW | INFO
        source="dataflow-engine",
        tags={"pipeline": "payment_pipeline", "step": "validate_payments"},
    )
    print(f"Created incident: {incident_id}")

Environment variables (used when constructor arguments are omitted)
-------------------------------------------------------------------
MOOGSOFT_ENDPOINT   Full URL of the Moogsoft HTTP event ingestion endpoint.
MOOGSOFT_API_KEY    Bearer / token credential for Moogsoft authentication.
MOOGSOFT_TIMEOUT    HTTP request timeout in seconds (default: 30).
"""

import json
import logging
import os
import time
import uuid
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Severity constants
# ---------------------------------------------------------------------------
SEVERITY_CRITICAL = "CRITICAL"
SEVERITY_HIGH     = "HIGH"
SEVERITY_MEDIUM   = "MEDIUM"
SEVERITY_LOW      = "LOW"
SEVERITY_INFO     = "INFO"

_VALID_SEVERITIES = {SEVERITY_CRITICAL, SEVERITY_HIGH, SEVERITY_MEDIUM,
                     SEVERITY_LOW, SEVERITY_INFO}

# Moogsoft numeric severity mapping (used in the event payload)
_SEVERITY_CODE: Dict[str, int] = {
    SEVERITY_CRITICAL: 5,
    SEVERITY_HIGH:     4,
    SEVERITY_MEDIUM:   3,
    SEVERITY_LOW:      2,
    SEVERITY_INFO:     1,
}


# ---------------------------------------------------------------------------

class IncidentCreationError(RuntimeError):
    """Raised when the Moogsoft API returns an error or the request fails."""


class MoogsoftIncidentConnector:
    """
    Sends HTTP events to Moogsoft AIOps which creates ServiceNow incidents.

    Parameters
    ----------
    endpoint : str, optional
        Moogsoft HTTP event ingestion URL.  Falls back to the
        ``MOOGSOFT_ENDPOINT`` environment variable.
    api_key : str, optional
        Moogsoft API key / bearer token.  Falls back to the
        ``MOOGSOFT_API_KEY`` environment variable.
    timeout : int, optional
        HTTP request timeout in seconds (default 30).
    default_source : str, optional
        Default value for the *source* field in every event (default
        ``"dataflow-engine"``).
    verify_ssl : bool, optional
        Whether to verify TLS certificates (default ``True``).
        Set to ``False`` only in development environments.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: int = 30,
        default_source: str = "dataflow-engine",
        verify_ssl: bool = True,
    ) -> None:
        self.endpoint = (endpoint or os.environ.get("MOOGSOFT_ENDPOINT", "")).rstrip("/")
        self.api_key  = api_key or os.environ.get("MOOGSOFT_API_KEY", "")
        self.timeout  = int(os.environ.get("MOOGSOFT_TIMEOUT", timeout))
        self.default_source = default_source
        self.verify_ssl = verify_ssl

        if not self.endpoint:
            raise ValueError(
                "Moogsoft endpoint is required. Pass 'endpoint' or set "
                "the MOOGSOFT_ENDPOINT environment variable."
            )
        if not self.api_key:
            raise ValueError(
                "Moogsoft API key is required. Pass 'api_key' or set "
                "the MOOGSOFT_API_KEY environment variable."
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_incident(
        self,
        summary: str,
        description: str = "",
        severity: str = SEVERITY_MEDIUM,
        source: Optional[str] = None,
        alert_key: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        service: str = "dataflow",
        additional_fields: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Send an event to Moogsoft and create a ServiceNow incident.

        Parameters
        ----------
        summary : str
            Short summary / title for the incident (maps to SNOW *short_description*).
        description : str, optional
            Detailed description (maps to SNOW *description*).
        severity : str, optional
            One of ``CRITICAL``, ``HIGH``, ``MEDIUM``, ``LOW``, ``INFO``.
        source : str, optional
            Originating system identifier.  Defaults to ``self.default_source``.
        alert_key : str, optional
            Unique de-duplication key for the alert.  Auto-generated if omitted.
        tags : dict, optional
            Arbitrary key/value pairs attached as custom fields in Moogsoft.
        service : str, optional
            Business service name forwarded to ServiceNow (default ``"dataflow"``).
        additional_fields : dict, optional
            Extra top-level fields merged into the event payload.

        Returns
        -------
        str
            The ``alert_key`` used in the submitted event (useful for correlation).

        Raises
        ------
        IncidentCreationError
            If the HTTP request fails or Moogsoft returns a non-2xx status.
        ValueError
            If an invalid severity string is provided.
        """
        severity = severity.upper()
        if severity not in _VALID_SEVERITIES:
            raise ValueError(
                f"Invalid severity '{severity}'. "
                f"Must be one of: {', '.join(sorted(_VALID_SEVERITIES))}"
            )

        if not alert_key:
            alert_key = str(uuid.uuid4())

        payload = self._build_payload(
            summary=summary,
            description=description,
            severity=severity,
            source=source or self.default_source,
            alert_key=alert_key,
            tags=tags or {},
            service=service,
            additional_fields=additional_fields or {},
        )

        LOG.info(
            "[INCIDENT] Sending Moogsoft event: alert_key=%s  severity=%s  summary=%r",
            alert_key, severity, summary,
        )

        self._post(payload)

        LOG.info("[INCIDENT] Event accepted by Moogsoft: alert_key=%s", alert_key)
        return alert_key

    def create_validation_incident(
        self,
        pipeline_name: str,
        step_id: str,
        invalid_count: int,
        severity: str = SEVERITY_HIGH,
        error_samples: Optional[list] = None,
        tags: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Convenience wrapper — creates an incident for a data validation failure.

        Parameters
        ----------
        pipeline_name : str
            Name of the dataflow pipeline / configuration.
        step_id : str
            The validate step ID that failed.
        invalid_count : int
            Number of rows that failed validation.
        severity : str, optional
            Incident severity (default ``HIGH``).
        error_samples : list, optional
            Up to 5 sample validation-error strings for the description.
        tags : dict, optional
            Additional tags merged with pipeline/step metadata.

        Returns
        -------
        str
            The ``alert_key`` used in the submitted event.
        """
        summary = (
            f"Data validation failed — {invalid_count} invalid row(s) "
            f"in pipeline '{pipeline_name}' step '{step_id}'"
        )
        sample_text = ""
        if error_samples:
            sample_text = "\n\nSample errors:\n" + "\n".join(
                f"  - {e}" for e in error_samples[:5]
            )
        description = (
            f"Pipeline : {pipeline_name}\n"
            f"Step     : {step_id}\n"
            f"Invalid  : {invalid_count} row(s)"
            f"{sample_text}"
        )
        merged_tags = {"pipeline": pipeline_name, "step": step_id,
                       "invalid_count": invalid_count}
        if tags:
            merged_tags.update(tags)

        return self.create_incident(
            summary=summary,
            description=description,
            severity=severity,
            alert_key=f"dataflow.{pipeline_name}.{step_id}.validation",
            tags=merged_tags,
            service="dataflow",
        )

    def create_last_run_file_missing_incident(
        self,
        pipeline_name: str,
        step_id: str,
        file_path: str,
        partition_column: str,
        severity: str = SEVERITY_CRITICAL,
        tags: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Convenience wrapper — creates a CRITICAL incident when the last run date
        file is not found, causing the job to be aborted.

        Parameters
        ----------
        pipeline_name : str
            Name of the dataflow pipeline / configuration.
        step_id : str
            The validate step ID that triggered the check.
        file_path : str
            Full path (local or S3) where the last run date file was expected.
        partition_column : str
            The partition column configured for date-partitioned processing.
        severity : str, optional
            Incident severity (default ``CRITICAL``).
        tags : dict, optional
            Additional tags merged with pipeline/step metadata.

        Returns
        -------
        str
            The ``alert_key`` used in the submitted event.
        """
        summary = (
            f"Last run date file missing — pipeline '{pipeline_name}' "
            f"step '{step_id}' aborted"
        )
        description = (
            f"Pipeline         : {pipeline_name}\n"
            f"Step             : {step_id}\n"
            f"Expected File    : {file_path}\n"
            f"Partition Column : {partition_column}\n\n"
            f"The dataflow job was aborted because the last run date file was not "
            f"found at the configured path. Please verify that the file exists and "
            f"is accessible, then re-run the pipeline."
        )
        merged_tags: Dict[str, Any] = {
            "pipeline":         pipeline_name,
            "step":             step_id,
            "missing_file":     file_path,
            "partition_column": partition_column,
        }
        if tags:
            merged_tags.update(tags)

        return self.create_incident(
            summary=summary,
            description=description,
            severity=severity,
            alert_key=f"dataflow.{pipeline_name}.{step_id}.last_run_file_missing",
            tags=merged_tags,
            service="dataflow",
        )

    def create_input_file_missing_incident(
        self,
        pipeline_name: str,
        input_name: str,
        file_path: str,
        severity: str = SEVERITY_CRITICAL,
        tags: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Convenience wrapper — creates a CRITICAL incident when a required
        input file is not found at the expected path, causing the job to abort.

        Parameters
        ----------
        pipeline_name : str
            Name of the dataflow pipeline / configuration.
        input_name : str
            The logical input name as defined in the config (e.g. 'HOGAN-INPUT').
        file_path : str
            Full path (local or S3) where the input file was expected.
        severity : str, optional
            Incident severity (default ``CRITICAL``).
        tags : dict, optional
            Additional tags merged with pipeline/input metadata.

        Returns
        -------
        str
            The ``alert_key`` used in the submitted event.
        """
        summary = (
            f"Input file not found — pipeline '{pipeline_name}' "
            f"input '{input_name}' aborted"
        )
        description = (
            f"Pipeline      : {pipeline_name}\n"
            f"Input Name    : {input_name}\n"
            f"Expected Path : {file_path}\n\n"
            f"The dataflow job was aborted because the required input file was not "
            f"found at the configured path. The file has not been created or delivered "
            f"to the expected location. Please verify that the upstream process has "
            f"completed successfully and the file exists, then re-run the pipeline."
        )
        merged_tags: Dict[str, Any] = {
            "pipeline":    pipeline_name,
            "input_name":  input_name,
            "missing_file": file_path,
        }
        if tags:
            merged_tags.update(tags)

        return self.create_incident(
            summary=summary,
            description=description,
            severity=severity,
            alert_key=f"dataflow.{pipeline_name}.{input_name}.input_file_missing",
            tags=merged_tags,
            service="dataflow",
        )

    def create_prev_day_check_incident(
        self,
        pipeline_name: str,
        input_name: str,
        file_path: str,
        expected_date: str,
        actual_date: str,
        severity: str = SEVERITY_CRITICAL,
        tags: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Convenience wrapper — creates a CRITICAL incident when the previous-day
        header check fails.

        Either the previous business day's file is missing or its header date
        does not match the expected business day.

        Parameters
        ----------
        pipeline_name : str
            Name of the dataflow pipeline / configuration.
        input_name : str
            The logical input name as defined in the config (e.g. 'HOGAN-INPUT').
        file_path : str
            Full path to the previous business day's input file that was checked.
        expected_date : str
            ISO-format date string (``YYYY-MM-DD``) that the header should have.
        actual_date : str
            ISO-format date string (``YYYY-MM-DD``) actually found in the header.
        severity : str, optional
            Incident severity (default ``CRITICAL``).
        tags : dict, optional
            Additional tags merged with pipeline/input metadata.

        Returns
        -------
        str
            The ``alert_key`` used in the submitted event.
        """
        summary = (
            f"Previous-day header check failed — pipeline '{pipeline_name}' "
            f"input '{input_name}' aborted"
        )
        description = (
            f"Pipeline           : {pipeline_name}\n"
            f"Input              : {input_name}\n"
            f"Previous Day File  : {file_path}\n"
            f"Expected Date      : {expected_date}\n"
            f"Actual Header Date : {actual_date}\n\n"
            f"The dataflow job was aborted because the previous business day's "
            f"input file header date does not match the expected date. Please "
            f"verify that the correct file was delivered and re-run the pipeline."
        )
        merged_tags: Dict[str, Any] = {
            "pipeline":      pipeline_name,
            "input":         input_name,
            "prev_day_file": file_path,
            "expected_date": expected_date,
            "actual_date":   actual_date,
        }
        if tags:
            merged_tags.update(tags)

        return self.create_incident(
            summary=summary,
            description=description,
            severity=severity,
            alert_key=f"dataflow.{pipeline_name}.{input_name}.prev_day_check_failed",
            tags=merged_tags,
            service="dataflow",
        )

    def create_record_count_check_incident(
        self,
        pipeline_name: str,
        step_id: str,
        input_file_path: str,
        expected_count: str,
        actual_count: str,
        severity: str = SEVERITY_CRITICAL,
        tags: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Convenience wrapper — creates a CRITICAL incident when the trailer
        record count does not match the actual number of loaded data records.

        Parameters
        ----------
        pipeline_name : str
            Name of the dataflow pipeline / configuration.
        step_id : str
            The validate step ID where the check was performed.
        input_file_path : str
            Display path of the input file whose trailer was checked.
        expected_count : str
            Record count extracted from the trailer field.
        actual_count : str
            Actual number of data records loaded from the input file.
        severity : str, optional
            Incident severity (default ``CRITICAL``).
        tags : dict, optional
            Additional tags merged with pipeline/step metadata.

        Returns
        -------
        str
            The ``alert_key`` used in the submitted event.
        """
        summary = (
            f"Record count mismatch — pipeline '{pipeline_name}' "
            f"step '{step_id}' aborted"
        )
        description = (
            f"Pipeline         : {pipeline_name}\n"
            f"Validate Step    : {step_id}\n"
            f"Input File       : {input_file_path}\n"
            f"Expected Count   : {expected_count}\n"
            f"Actual Count     : {actual_count}\n\n"
            f"The dataflow job was aborted because the trailer record count does "
            f"not match the actual number of data records loaded from the input file. "
            f"Please verify that the input file is complete and re-run the pipeline."
        )
        merged_tags: Dict[str, Any] = {
            "pipeline":       pipeline_name,
            "step":           step_id,
            "input_file":     input_file_path,
            "expected_count": expected_count,
            "actual_count":   actual_count,
        }
        if tags:
            merged_tags.update(tags)

        return self.create_incident(
            summary=summary,
            description=description,
            severity=severity,
            alert_key=f"dataflow.{pipeline_name}.{step_id}.record_count_check_failed",
            tags=merged_tags,
            service="dataflow",
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_payload(
        summary: str,
        description: str,
        severity: str,
        source: str,
        alert_key: str,
        tags: Dict[str, Any],
        service: str,
        additional_fields: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build the Moogsoft HTTP event payload."""
        payload: Dict[str, Any] = {
            # Core Moogsoft event fields
            "alert_key":   alert_key,
            "description": summary,        # maps to SNOW short_description
            "severity":    _SEVERITY_CODE[severity],
            "source":      source,
            "service":     service,
            "manager":     "dataflow-engine",
            "type":        "data_validation",
            "time":        int(time.time()),

            # Extended fields for ServiceNow
            "snow_short_description": summary,
            "snow_description":       description,
            "snow_category":          "Software",
            "snow_subcategory":       "Data Quality",
        }

        # Attach tags as custom Moogsoft fields
        if tags:
            payload["custom_info"] = {
                str(k): str(v) for k, v in tags.items()
            }

        # Merge any caller-supplied top-level fields (allow override)
        payload.update(additional_fields)
        return payload

    def _post(self, payload: Dict[str, Any]) -> None:
        """Execute the HTTP POST to the Moogsoft endpoint."""
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type":  "application/json",
            "Accept":        "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }

        req = Request(self.endpoint, data=body, headers=headers, method="POST")

        try:
            import ssl
            ctx = ssl.create_default_context() if self.verify_ssl else ssl._create_unverified_context()
            with urlopen(req, timeout=self.timeout, context=ctx) as resp:
                status = resp.status
                resp_body = resp.read().decode("utf-8", errors="replace")

            if status not in range(200, 300):
                raise IncidentCreationError(
                    f"Moogsoft returned HTTP {status}: {resp_body[:500]}"
                )

            LOG.debug("[INCIDENT] Moogsoft response HTTP %d: %s", status, resp_body[:200])

        except HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            raise IncidentCreationError(
                f"Moogsoft HTTP error {exc.code}: {body_text[:500]}"
            ) from exc

        except URLError as exc:
            raise IncidentCreationError(
                f"Failed to reach Moogsoft endpoint '{self.endpoint}': {exc.reason}"
            ) from exc
