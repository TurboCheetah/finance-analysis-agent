from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

from sqlalchemy.orm import Session

from finance_analysis_agent.dedupe import TxnDedupeMatchRequest, txn_dedupe_match
from finance_analysis_agent.db.models import Account, Transaction
from finance_analysis_agent.utils.time import utcnow


@dataclass(slots=True)
class _Metrics:
    precision: float | None
    recall: float | None


def _pair_key(txn_a_id: str, txn_b_id: str) -> tuple[str, str]:
    if txn_a_id <= txn_b_id:
        return (txn_a_id, txn_b_id)
    return (txn_b_id, txn_a_id)


def _metrics(predicted: set[tuple[str, str]], expected: set[tuple[str, str]]) -> _Metrics:
    true_positive = len(predicted & expected)
    precision = None if not predicted else (true_positive / len(predicted))
    recall = None if not expected else (true_positive / len(expected))
    return _Metrics(precision=precision, recall=recall)


def _seed_account(session: Session) -> None:
    session.add(Account(id="acct-1", name="Checking", type="checking", currency="USD"))


def _seed_fixture_transactions(session: Session, fixture: dict[str, object]) -> None:
    now = utcnow()
    for row in fixture["transactions"]:
        session.add(
            Transaction(
                id=row["id"],
                account_id=row["account_id"],
                posted_date=date.fromisoformat(row["posted_date"]),
                effective_date=date.fromisoformat(row["posted_date"]),
                amount=Decimal(row["amount"]),
                currency=row["currency"],
                original_amount=Decimal(row["amount"]),
                original_currency=row["currency"],
                pending_status=row["pending_status"],
                original_statement=row["original_statement"],
                merchant_id=None,
                category_id=None,
                excluded=False,
                notes=None,
                source_kind=row["source_kind"],
                source_transaction_id=f"src-{row['id']}",
                import_batch_id=None,
                transfer_group_id=None,
                created_at=now,
                updated_at=now,
            )
        )


def test_dedupe_precision_recall_from_labeled_fixture(db_session: Session) -> None:
    fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "dedupe" / "labeled_pairs.json"
    fixture = json.loads(fixture_path.read_text())

    _seed_account(db_session)
    _seed_fixture_transactions(db_session, fixture)
    db_session.flush()

    result = txn_dedupe_match(
        TxnDedupeMatchRequest(
            actor="metrics-tester",
            reason="fixture metrics",
            include_pending=False,
            soft_review_threshold=0.75,
            soft_autolink_threshold=1.0,
        ),
        db_session,
    )

    expected_hard = {
        _pair_key(pair[0], pair[1]) for pair in fixture["expected_hard_duplicates"]
    }
    expected_soft = {
        _pair_key(pair[0], pair[1]) for pair in fixture["expected_soft_duplicates"]
    }

    predicted_hard = {
        _pair_key(candidate.txn_a_id, candidate.txn_b_id)
        for candidate in result.candidates
        if candidate.classification == "hard" and candidate.decision == "duplicate"
    }
    predicted_soft = {
        _pair_key(candidate.txn_a_id, candidate.txn_b_id)
        for candidate in result.candidates
        if candidate.classification == "soft" and candidate.score >= 0.75
    }

    hard_metrics = _metrics(predicted_hard, expected_hard)
    soft_metrics = _metrics(predicted_soft, expected_soft)

    assert hard_metrics.precision == 1.0
    assert hard_metrics.recall == 1.0
    assert soft_metrics.precision is not None and soft_metrics.precision >= 0.9
    assert soft_metrics.recall is not None and soft_metrics.recall >= 0.9
