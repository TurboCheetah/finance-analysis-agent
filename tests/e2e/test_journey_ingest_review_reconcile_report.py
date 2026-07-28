from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import select

from finance_analysis_agent.dedupe import TxnDedupeMatchRequest, txn_dedupe_match
from finance_analysis_agent.db.models import DedupeCandidate, ReviewItem, RunMetadata, Transaction
from finance_analysis_agent.pdf_contract import (
    DeterministicFakePdfSubagentAdapter,
    PdfDiagnostics,
    PdfExtractedRow,
    PdfExtractionTier,
    PdfSubagentRequest,
    PdfSubagentResponse,
    run_pdf_subagent_handoff,
)
from finance_analysis_agent.reconcile import AccountReconcileRequest, account_reconcile
from finance_analysis_agent.reporting import ReportType, ReportingGenerateRequest, reporting_generate
from finance_analysis_agent.review_queue import (
    BulkActionType,
    BulkTriageRequest,
    ReviewQueueListRequest,
    bulk_triage,
    list_review_items,
)
from tests.e2e.helpers import (
    persist_artifact,
    seed_account,
    seed_balance_snapshot,
    seed_category,
    seed_statement,
    seed_transaction,
    write_json_artifact,
)

pytestmark = pytest.mark.e2e


def _pdf_request(statement_path: Path) -> PdfSubagentRequest:
    return PdfSubagentRequest(
        contract_version="1.0.0",
        statement_path=str(statement_path),
        account_id="acct-pdf",
        schema_version="1.0.0",
        actor="e2e-pdf",
        confidence_threshold=0.8,
        source_ref="fixtures/e2e-statement.pdf",
    )


def _pdf_response() -> PdfSubagentResponse:
    return PdfSubagentResponse(
        contract_version="1.0.0",
        subagent_version_hash="e2e-subagent",
        extraction_tiers_used=[PdfExtractionTier.TEXT_HEURISTIC],
        rows=[
            PdfExtractedRow(
                account_id="acct-pdf",
                posted_date="2026-01-05",
                amount="-20.00",
                currency="USD",
                pending_status="posted",
                confidence=0.95,
                parse_status="parsed",
                page_no=1,
                row_no=1,
                source_transaction_id="pdf-valid-1",
                original_statement="PDF VALID COFFEE",
            ),
            PdfExtractedRow(
                account_id="acct-pdf",
                posted_date="2026-01-06",
                amount="-8.50",
                currency="USD",
                pending_status="posted",
                confidence=0.40,
                parse_status="parsed",
                page_no=1,
                row_no=2,
                source_transaction_id="pdf-low-confidence-1",
                original_statement="PDF LOW CONFIDENCE",
            ),
            PdfExtractedRow(
                account_id="acct-pdf",
                posted_date="2026-01-07",
                amount="-5.25",
                currency="USD",
                pending_status="posted",
                confidence=0.90,
                parse_status="parse_error",
                error_code="bad_date",
                page_no=1,
                row_no=3,
                source_transaction_id="pdf-parse-error-1",
                original_statement="PDF BAD DATE",
            ),
        ],
        diagnostics=PdfDiagnostics(
            run_summary={"pages": 1},
            page_notes=[{"page_no": 1, "note": "mixed-confidence rows"}],
        ),
    )


def test_journey_ingest_review_reconcile_report(db_session, tmp_path: Path) -> None:
    seed_account(db_session, account_id="acct-pdf", name="PDF Checking")
    seed_account(db_session, account_id="acct-recon", name="Reconciliation Checking")
    seed_category(db_session, category_id="cat-income", name="Income")
    seed_statement(
        db_session,
        statement_id="stmt-recon-jan",
        account_id="acct-recon",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        ending_balance="130.00",
    )
    seed_balance_snapshot(
        db_session,
        snapshot_id="snap-opening-jan",
        account_id="acct-recon",
        snapshot_date=date(2026, 1, 1),
        balance="100.00",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-recon-1",
        account_id="acct-recon",
        posted_date=date(2026, 1, 10),
        amount="20.00",
        category_id="cat-income",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-recon-2",
        account_id="acct-recon",
        posted_date=date(2026, 1, 20),
        amount="10.00",
        category_id="cat-income",
    )
    db_session.flush()

    statement_path = tmp_path / "e2e-statement.pdf"
    statement_path.write_bytes(b"%PDF-1.4 e2e")
    pdf_result = run_pdf_subagent_handoff(
        _pdf_request(statement_path),
        DeterministicFakePdfSubagentAdapter(response=_pdf_response()),
        db_session,
    )
    db_session.flush()

    assert pdf_result.ok is True
    assert pdf_result.status == "success_with_warnings"
    assert pdf_result.inserted_rows == 1
    assert pdf_result.skipped_rows == 2

    pdf_run = db_session.get(RunMetadata, pdf_result.run_metadata_id)
    assert pdf_run is not None
    assert pdf_run.status == "success_with_warnings"
    assert pdf_run.diagnostics_json is not None
    assert pdf_run.diagnostics_json["review_summary"]["total_items_created"] == 3

    pdf_transactions = db_session.scalars(
        select(Transaction).where(Transaction.account_id == "acct-pdf").order_by(Transaction.id.asc())
    ).all()
    assert [txn.source_transaction_id for txn in pdf_transactions] == ["pdf-valid-1"]

    seed_transaction(
        db_session,
        transaction_id="txn-pending-link",
        account_id="acct-pdf",
        posted_date=date(2026, 1, 20),
        amount="100.00",
        pending_status="pending",
        source_kind="csv",
        original_statement="COFFEE ROASTERS",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-posted-link",
        account_id="acct-pdf",
        posted_date=date(2026, 1, 21),
        amount="100.50",
        pending_status="posted",
        source_kind="csv",
        original_statement="coffee roasters",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-cross-source-a",
        account_id="acct-pdf",
        posted_date=date(2026, 1, 24),
        amount="45.00",
        pending_status="pending",
        source_kind="csv",
        original_statement="STREAMING SERVICE",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-cross-source-b",
        account_id="acct-pdf",
        posted_date=date(2026, 1, 25),
        amount="45.25",
        pending_status="posted",
        source_kind="pdf",
        original_statement="streaming service",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-soft-a",
        account_id="acct-pdf",
        posted_date=date(2026, 1, 15),
        amount="82.10",
        source_kind="pdf",
        original_statement="GROCERY OUTLET WEST",
    )
    seed_transaction(
        db_session,
        transaction_id="txn-soft-b",
        account_id="acct-pdf",
        posted_date=date(2026, 1, 16),
        amount="82.50",
        source_kind="pdf",
        original_statement="GROCERY OUTLET W",
    )
    db_session.flush()

    pending_posted_result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="e2e-dedupe",
            reason="pending posted link",
            include_pending=True,
            scope_transaction_ids=["txn-pending-link", "txn-posted-link"],
        ),
        db_session,
    )
    cross_source_result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="e2e-dedupe",
            reason="cross source guard",
            include_pending=True,
            scope_transaction_ids=["txn-cross-source-a", "txn-cross-source-b"],
        ),
        db_session,
    )
    soft_match_result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="e2e-dedupe",
            reason="soft match queue",
            soft_review_threshold=0.75,
            soft_autolink_threshold=1.0,
            scope_transaction_ids=["txn-soft-a", "txn-soft-b"],
        ),
        db_session,
    )
    db_session.flush()

    assert pending_posted_result.hard_auto_linked == 1
    assert pending_posted_result.candidates[0].policy_flags["pending_posted_link"] is True
    assert cross_source_result.hard_auto_linked == 0
    assert cross_source_result.soft_queued == 1
    assert cross_source_result.candidates[0].policy_flags["cross_source_review_only_applied"] is True
    assert soft_match_result.soft_queued == 1
    assert soft_match_result.candidates[0].classification == "soft"

    review_list = list_review_items(ReviewQueueListRequest(), db_session)
    reason_codes = {item.reason_code for item in review_list.items}
    assert {
        "low_confidence_page",
        "low_confidence_row",
        "parse_error_row",
        "dedupe.cross_source_review_only",
        "dedupe.soft_match",
    }.issubset(reason_codes)

    cross_source_review = db_session.scalar(
        select(ReviewItem).where(ReviewItem.reason_code == "dedupe.cross_source_review_only")
    )
    assert cross_source_review is not None
    triage_result = bulk_triage(
        BulkTriageRequest(
            action=BulkActionType.MARK_DUPLICATE,
            review_item_ids=[cross_source_review.id],
            actor="e2e-reviewer",
            reason="confirmed cross-source duplicate",
        ),
        db_session,
    )
    db_session.flush()

    assert triage_result.updated == 1
    triaged_candidate = db_session.get(DedupeCandidate, cross_source_review.ref_id)
    assert triaged_candidate is not None
    assert triaged_candidate.decision == "duplicate"

    reconciliation_result = account_reconcile(
        AccountReconcileRequest(
            account_id="acct-recon",
            period_start=date(2026, 1, 1),
            period_end=date(2026, 1, 31),
            actor="e2e-reconciler",
            reason="monthly close",
        ),
        db_session,
    )
    db_session.flush()

    assert reconciliation_result.status == "pass"
    assert reconciliation_result.delta == Decimal("0.00")
    assert reconciliation_result.trust_score >= 0.9

    reporting_result = reporting_generate(
        ReportingGenerateRequest(
            actor="e2e-reporter",
            reason="workflow verification",
            period_month="2026-01",
            report_types=[
                ReportType.CASH_FLOW,
                ReportType.CATEGORY_TRENDS,
                ReportType.NET_WORTH,
                ReportType.QUALITY_TRUST_DASHBOARD,
            ],
        ),
        db_session,
    )
    db_session.flush()

    assert {report.report_type for report in reporting_result.reports} == {
        ReportType.CASH_FLOW,
        ReportType.CATEGORY_TRENDS,
        ReportType.NET_WORTH,
        ReportType.QUALITY_TRUST_DASHBOARD,
    }
    assert all(report.payload_hash for report in reporting_result.reports)

    summary_path = write_json_artifact(
        tmp_path / "journey-ingest-review-reconcile-report.json",
        {
            "pdf_run_metadata_id": pdf_result.run_metadata_id,
            "pending_posted_candidate_id": pending_posted_result.candidates[0].dedupe_candidate_id,
            "cross_source_candidate_id": cross_source_result.candidates[0].dedupe_candidate_id,
            "soft_candidate_id": soft_match_result.candidates[0].dedupe_candidate_id,
            "reconciliation_id": reconciliation_result.reconciliation_id,
            "report_types": sorted(report.report_type.value for report in reporting_result.reports),
        },
    )
    persist_artifact("journey-ingest-review-reconcile-report.json", summary_path)
