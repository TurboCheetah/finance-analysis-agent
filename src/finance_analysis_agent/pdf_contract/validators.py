"""Contract validation helpers for PDF subagent request/response payloads."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from finance_analysis_agent.pdf_contract.types import (
    PdfContractError,
    PdfDiagnostics,
    PdfExtractionTier,
    PdfExtractedRow,
    PdfSubagentRequest,
    PdfSubagentResponse,
)

SEMVER_RE = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")


def _error(code: str, message: str, stage: str, **details: Any) -> PdfContractError:
    return PdfContractError(code=code, message=message, stage=stage, details=details)


def parse_semver_major(version: str) -> int | None:
    match = SEMVER_RE.fullmatch(version.strip()) if isinstance(version, str) else None
    if not match:
        return None
    return int(match.group("major"))


def validate_contract_version(expected: str, actual: str) -> PdfContractError | None:
    """Validate major-version compatibility between orchestrator and subagent."""

    expected_major = parse_semver_major(expected)
    actual_major = parse_semver_major(actual)
    if expected_major is None or actual_major is None:
        return _error(
            code="version_incompatible",
            message="Contract version must use semantic version format",
            stage="version_validation",
            expected=expected,
            actual=actual,
        )
    if expected_major != actual_major:
        return _error(
            code="version_incompatible",
            message="Contract major version mismatch",
            stage="version_validation",
            expected=expected,
            actual=actual,
        )
    return None


def validate_pdf_subagent_request(request: PdfSubagentRequest) -> list[PdfContractError]:
    errors: list[PdfContractError] = []

    if parse_semver_major(request.contract_version) is None:
        errors.append(
            _error(
                code="request_invalid",
                message="contract_version must be semantic version",
                stage="request_validation",
                field="contract_version",
                value=request.contract_version,
            )
        )

    if not request.statement_path or not Path(request.statement_path).is_absolute():
        errors.append(
            _error(
                code="request_invalid",
                message="statement_path must be an absolute local path",
                stage="request_validation",
                field="statement_path",
                value=request.statement_path,
            )
        )

    for field_name in ("account_id", "schema_version", "actor"):
        value = getattr(request, field_name)
        if not isinstance(value, str) or not value.strip():
            errors.append(
                _error(
                    code="request_invalid",
                    message=f"{field_name} is required",
                    stage="request_validation",
                    field=field_name,
                )
            )

    threshold = request.confidence_threshold
    if not isinstance(threshold, (int, float)) or threshold < 0 or threshold > 1:
        errors.append(
            _error(
                code="request_invalid",
                message="confidence_threshold must be between 0 and 1",
                stage="request_validation",
                field="confidence_threshold",
                value=threshold,
            )
        )

    if request.page_range is not None:
        if (
            not isinstance(request.page_range, tuple)
            or len(request.page_range) != 2
            or request.page_range[0] <= 0
            or request.page_range[1] < request.page_range[0]
        ):
            errors.append(
                _error(
                    code="request_invalid",
                    message="page_range must be (start, end) positive tuple",
                    stage="request_validation",
                    field="page_range",
                    value=request.page_range,
                )
            )

    return errors


def _validate_row(row: PdfExtractedRow, row_index: int) -> list[PdfContractError]:
    errors: list[PdfContractError] = []

    if row.confidence is not None and (
        not isinstance(row.confidence, (int, float)) or row.confidence < 0 or row.confidence > 1
    ):
        errors.append(
            _error(
                code="response_invalid",
                message="row confidence must be between 0 and 1",
                stage="response_validation",
                row_index=row_index,
                field="confidence",
                value=row.confidence,
            )
        )

    raw_parse_status = row.parse_status
    if not isinstance(raw_parse_status, str):
        errors.append(
            _error(
                code="response_invalid",
                message="row parse_status must be a string",
                stage="response_validation",
                row_index=row_index,
                field="parse_status",
            )
        )
        return errors

    parse_status = raw_parse_status.strip()
    if not parse_status:
        errors.append(
            _error(
                code="response_invalid",
                message="row parse_status is required",
                stage="response_validation",
                row_index=row_index,
                field="parse_status",
                value=raw_parse_status,
            )
        )
        return errors

    if parse_status == "parsed":
        required_fields = {
            "posted_date": row.posted_date,
            "amount": row.amount,
            "currency": row.currency,
            "pending_status": row.pending_status,
        }
        for field_name, value in required_fields.items():
            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(
                    _error(
                        code="response_invalid",
                        message=f"parsed row missing required field: {field_name}",
                        stage="response_validation",
                        row_index=row_index,
                        field=field_name,
                    )
                )

    for field_name in ("page_no", "row_no"):
        value = getattr(row, field_name)
        if value is not None and (not isinstance(value, int) or value <= 0):
            errors.append(
                _error(
                    code="response_invalid",
                    message=f"{field_name} must be a positive integer when set",
                    stage="response_validation",
                    row_index=row_index,
                    field=field_name,
                    value=value,
                )
            )

    return errors


def validate_pdf_subagent_response(response: PdfSubagentResponse) -> list[PdfContractError]:
    errors: list[PdfContractError] = []

    if parse_semver_major(response.contract_version) is None:
        errors.append(
            _error(
                code="response_invalid",
                message="contract_version must be semantic version",
                stage="response_validation",
                field="contract_version",
                value=response.contract_version,
            )
        )

    if not isinstance(response.subagent_version_hash, str) or not response.subagent_version_hash.strip():
        errors.append(
            _error(
                code="response_invalid",
                message="subagent_version_hash is required",
                stage="response_validation",
                field="subagent_version_hash",
            )
        )

    if not isinstance(response.diagnostics, PdfDiagnostics):
        errors.append(
            _error(
                code="response_invalid",
                message="diagnostics must be a PdfDiagnostics object",
                stage="response_validation",
                field="diagnostics",
            )
        )

    if not isinstance(response.rows, list):
        errors.append(
            _error(
                code="response_invalid",
                message="rows must be a list",
                stage="response_validation",
                field="rows",
            )
        )
        return errors

    if not isinstance(response.extraction_tiers_used, list) or len(response.extraction_tiers_used) == 0:
        errors.append(
            _error(
                code="response_invalid",
                message="extraction_tiers_used must be a non-empty list",
                stage="response_validation",
                field="extraction_tiers_used",
            )
        )
    else:
        valid_tiers = {tier.value for tier in PdfExtractionTier}
        for tier in response.extraction_tiers_used:
            tier_value = tier.value if isinstance(tier, PdfExtractionTier) else str(tier)
            if tier_value not in valid_tiers:
                errors.append(
                    _error(
                        code="response_invalid",
                        message="unknown extraction tier",
                        stage="response_validation",
                        field="extraction_tiers_used",
                        value=tier_value,
                    )
                )

    for row_index, row in enumerate(response.rows, start=1):
        if not isinstance(row, PdfExtractedRow):
            errors.append(
                _error(
                    code="response_invalid",
                    message="row must be PdfExtractedRow",
                    stage="response_validation",
                    row_index=row_index,
                )
            )
            continue
        errors.extend(_validate_row(row, row_index))

    return errors
