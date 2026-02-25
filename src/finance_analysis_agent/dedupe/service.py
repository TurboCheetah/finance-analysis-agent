"""Service-layer transaction dedupe matching with hard/soft candidate generation."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from sqlalchemy import and_, case, null, or_, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from finance_analysis_agent.dedupe.types import (
    DedupeCandidateResult,
    DedupeScoreBreakdown,
    TxnDedupeMatchRequest,
    TxnDedupeMatchResult,
)
from finance_analysis_agent.db.models import (
    DedupeCandidate,
    DedupeCandidateEvent,
    Merchant,
    ReviewItem,
    Transaction,
)
from finance_analysis_agent.review_queue.types import ReviewItemStatus, ReviewSource
from finance_analysis_agent.utils.time import utcnow

_ACTIVE_REVIEW_STATUSES = (
    ReviewItemStatus.TO_REVIEW.value,
    ReviewItemStatus.IN_PROGRESS.value,
)
_SOFT_REVIEW_REASON = "dedupe.soft_match"
_CROSS_SOURCE_REVIEW_REASON = "dedupe.cross_source_review_only"
_SOFT_REVIEW_ITEM_TYPE = "dedupe_candidate_suggestion"
_SOFT_REVIEW_REF_TABLE = "dedupe_candidates"
_DUPLICATE_DECISION = "duplicate"
_SOFT_SUGGESTION_KIND = "dedupe_decision"
_PENDING_POSTED_SIMILARITY_FLOOR = 0.9
_PENDING_STATUS = "pending"
_POSTED_STATUS = "posted"

_CANDIDATE_CREATED_EVENT_TYPE = "dedupe_candidate.created"
_CANDIDATE_UPDATED_EVENT_TYPE = "dedupe_candidate.updated"
_CANDIDATE_DECISION_CHANGED_EVENT_TYPE = "dedupe_candidate.decision_changed"

_TEXT_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_TEXT_WHITESPACE_RE = re.compile(r"\s+")

_AMOUNT_WEIGHT = 0.45
_DATE_WEIGHT = 0.20
_MERCHANT_PAYEE_WEIGHT = 0.25
_STATEMENT_WEIGHT = 0.05
_SOURCE_KIND_WEIGHT = 0.05

_ERR_REQUIRED = "{field_name} is required"
_ERR_DECIMAL_COMPAT = "{field_name} must be a decimal-compatible value"
_ERR_NON_NEGATIVE = "{field_name} must be >= 0"
_ERR_BETWEEN_0_1 = "{field_name} must be between 0 and 1"
_ERR_WHOLE_NUMBER = "{field_name} must be a whole number"
_ERR_REVIEW_THRESHOLD_ORDER = "soft_review_threshold must be <= soft_autolink_threshold"
_ERR_LIMIT_POSITIVE = "limit must be > 0"


class _DedupeValidationError(ValueError):
    """Raised when dedupe request fields fail validation."""


class _DedupePersistenceError(RuntimeError):
    """Raised when dedupe persistence operations cannot resolve expected rows."""

    @staticmethod
    def unresolved_pair(pair_key: tuple[str, str]) -> "_DedupePersistenceError":
        return _DedupePersistenceError(f"Failed to resolve DedupeCandidate for pair {pair_key}")

    @staticmethod
    def unresolved_candidate(candidate_id: str) -> "_DedupePersistenceError":
        return _DedupePersistenceError(f"Failed to load DedupeCandidate {candidate_id}")

    @staticmethod
    def unresolved_active_review(candidate_id: str) -> "_DedupePersistenceError":
        return _DedupePersistenceError(
            "Failed to resolve active dedupe review item after upsert conflict "
            f"for candidate {candidate_id}"
        )

    @staticmethod
    def unresolved_review_item(review_item_id: str) -> "_DedupePersistenceError":
        return _DedupePersistenceError(f"Failed to load ReviewItem {review_item_id}")


@dataclass(slots=True)
class _ValidatedRequest:
    actor: str
    reason: str
    scope_transaction_ids: list[str]
    include_pending: bool
    hard_date_window_days: int
    soft_candidate_window_days: int
    soft_review_threshold: float
    soft_autolink_threshold: float
    pending_posted_window_days: int
    pending_amount_tolerance_pct: float
    pending_amount_tolerance_abs: Decimal
    cross_source_review_only: bool
    limit: int


@dataclass(slots=True)
class _TransactionView:
    id: str
    account_id: str
    posted_date: date
    amount: Decimal
    currency: str
    source_kind: str
    pending_status: str
    original_statement: str
    merchant_name: str
    normalized_payee: str
    normalized_statement: str


@dataclass(slots=True)
class _PendingPostedMatchEvaluation:
    is_pending_posted_pair: bool
    is_match: bool
    date_delta_days: int
    amount_delta: Decimal
    amount_tolerance: Decimal
    payee_similarity: float
    payee_exact: bool


def _normalize_scope_ids(scope_transaction_ids: list[str]) -> list[str]:
    normalized = {transaction_id.strip() for transaction_id in scope_transaction_ids if transaction_id.strip()}
    return sorted(normalized)


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    normalized = _TEXT_NON_ALNUM_RE.sub(" ", value.casefold())
    normalized = _TEXT_WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized


def _parse_non_empty(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise _DedupeValidationError(_ERR_REQUIRED.format(field_name=field_name))
    return normalized


def _parse_non_negative_decimal(value: object, *, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise _DedupeValidationError(_ERR_DECIMAL_COMPAT.format(field_name=field_name)) from exc
    if not parsed.is_finite():
        raise _DedupeValidationError(_ERR_DECIMAL_COMPAT.format(field_name=field_name))
    if parsed < 0:
        raise _DedupeValidationError(_ERR_NON_NEGATIVE.format(field_name=field_name))
    return parsed


def _parse_non_negative_int(value: object, *, field_name: str) -> int:
    parsed = _parse_non_negative_decimal(value, field_name=field_name)
    if parsed != parsed.to_integral_value():
        raise _DedupeValidationError(_ERR_WHOLE_NUMBER.format(field_name=field_name))
    return int(parsed)


def _normalized_pending_status(value: str) -> str:
    return value.strip().casefold()


def _validate_request(request: TxnDedupeMatchRequest) -> _ValidatedRequest:
    actor = _parse_non_empty(request.actor, field_name="actor")
    reason = _parse_non_empty(request.reason, field_name="reason")

    hard_date_window_days = _parse_non_negative_int(
        request.hard_date_window_days,
        field_name="hard_date_window_days",
    )
    soft_candidate_window_days = _parse_non_negative_int(
        request.soft_candidate_window_days,
        field_name="soft_candidate_window_days",
    )

    soft_review_threshold = float(request.soft_review_threshold)
    soft_autolink_threshold = float(request.soft_autolink_threshold)
    if not math.isfinite(soft_review_threshold) or soft_review_threshold < 0 or soft_review_threshold > 1:
        raise _DedupeValidationError(_ERR_BETWEEN_0_1.format(field_name="soft_review_threshold"))
    if not math.isfinite(soft_autolink_threshold) or soft_autolink_threshold < 0 or soft_autolink_threshold > 1:
        raise _DedupeValidationError(_ERR_BETWEEN_0_1.format(field_name="soft_autolink_threshold"))
    if soft_review_threshold > soft_autolink_threshold:
        raise _DedupeValidationError(_ERR_REVIEW_THRESHOLD_ORDER)

    pending_posted_window_days = _parse_non_negative_int(
        request.pending_posted_window_days,
        field_name="pending_posted_window_days",
    )

    pending_amount_tolerance_pct = float(request.pending_amount_tolerance_pct)
    if (
        not math.isfinite(pending_amount_tolerance_pct)
        or pending_amount_tolerance_pct < 0
        or pending_amount_tolerance_pct > 1
    ):
        raise _DedupeValidationError(_ERR_BETWEEN_0_1.format(field_name="pending_amount_tolerance_pct"))

    pending_amount_tolerance_abs = _parse_non_negative_decimal(
        request.pending_amount_tolerance_abs,
        field_name="pending_amount_tolerance_abs",
    )

    limit = _parse_non_negative_int(request.limit, field_name="limit")
    if limit == 0:
        raise _DedupeValidationError(_ERR_LIMIT_POSITIVE)

    return _ValidatedRequest(
        actor=actor,
        reason=reason,
        scope_transaction_ids=_normalize_scope_ids(request.scope_transaction_ids),
        include_pending=bool(request.include_pending),
        hard_date_window_days=hard_date_window_days,
        soft_candidate_window_days=soft_candidate_window_days,
        soft_review_threshold=soft_review_threshold,
        soft_autolink_threshold=soft_autolink_threshold,
        pending_posted_window_days=pending_posted_window_days,
        pending_amount_tolerance_pct=pending_amount_tolerance_pct,
        pending_amount_tolerance_abs=pending_amount_tolerance_abs,
        cross_source_review_only=bool(request.cross_source_review_only),
        limit=limit,
    )


def _pair_key(txn_a_id: str, txn_b_id: str) -> tuple[str, str]:
    if txn_a_id <= txn_b_id:
        return (txn_a_id, txn_b_id)
    return (txn_b_id, txn_a_id)


def _token_set(value: str) -> set[str]:
    if not value:
        return set()
    return {token for token in value.split(" ") if token}


def _jaccard_similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    left_tokens = _token_set(left)
    right_tokens = _token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0
    union_size = len(left_tokens | right_tokens)
    if union_size == 0:
        return 0.0
    return len(left_tokens & right_tokens) / union_size


def _compute_soft_score(
    left: _TransactionView,
    right: _TransactionView,
    *,
    soft_candidate_window_days: int,
) -> DedupeScoreBreakdown:
    amount_delta = abs(left.amount - right.amount)
    if amount_delta == Decimal("0"):
        amount_factor = 1.0
    else:
        denominator = max(abs(left.amount), abs(right.amount), Decimal("1"))
        amount_factor = max(0.0, 1.0 - float(amount_delta / denominator))

    day_delta = abs((left.posted_date - right.posted_date).days)
    if soft_candidate_window_days == 0:
        date_factor = 1.0 if day_delta == 0 else 0.0
    else:
        date_factor = max(0.0, 1.0 - (day_delta / soft_candidate_window_days))

    merchant_payee_factor = _jaccard_similarity(left.normalized_payee, right.normalized_payee)
    statement_factor = _jaccard_similarity(left.normalized_statement, right.normalized_statement)
    source_kind_factor = 1.0 if left.source_kind == right.source_kind else 0.0

    total_score = (
        (amount_factor * _AMOUNT_WEIGHT)
        + (date_factor * _DATE_WEIGHT)
        + (merchant_payee_factor * _MERCHANT_PAYEE_WEIGHT)
        + (statement_factor * _STATEMENT_WEIGHT)
        + (source_kind_factor * _SOURCE_KIND_WEIGHT)
    )
    total_score = max(0.0, min(1.0, total_score))
    return DedupeScoreBreakdown(
        amount_factor=amount_factor,
        date_factor=date_factor,
        merchant_payee_factor=merchant_payee_factor,
        statement_factor=statement_factor,
        source_kind_factor=source_kind_factor,
        total_score=total_score,
        details={
            "amount_delta": str(amount_delta),
            "date_delta_days": day_delta,
            "left_payee": left.normalized_payee,
            "right_payee": right.normalized_payee,
        },
    )


def _is_hard_match(
    left: _TransactionView,
    right: _TransactionView,
    *,
    hard_date_window_days: int,
) -> bool:
    if left.amount != right.amount:
        return False

    day_delta = abs((left.posted_date - right.posted_date).days)
    if day_delta > hard_date_window_days:
        return False

    if not left.normalized_payee or not right.normalized_payee:
        return False
    return left.normalized_payee == right.normalized_payee


def _evaluate_pending_posted_match(
    left: _TransactionView,
    right: _TransactionView,
    *,
    validated: _ValidatedRequest,
) -> _PendingPostedMatchEvaluation:
    left_status = _normalized_pending_status(left.pending_status)
    right_status = _normalized_pending_status(right.pending_status)
    statuses = {left_status, right_status}
    is_pending_posted_pair = statuses == {_PENDING_STATUS, _POSTED_STATUS}

    day_delta = abs((left.posted_date - right.posted_date).days)
    if not is_pending_posted_pair:
        return _PendingPostedMatchEvaluation(
            is_pending_posted_pair=False,
            is_match=False,
            date_delta_days=day_delta,
            amount_delta=Decimal("0"),
            amount_tolerance=Decimal("0"),
            payee_similarity=0.0,
            payee_exact=False,
        )

    amount_delta = abs(left.amount - right.amount)
    payee_similarity = _jaccard_similarity(left.normalized_payee, right.normalized_payee)
    payee_exact = bool(left.normalized_payee and right.normalized_payee and left.normalized_payee == right.normalized_payee)
    max_amount = max(abs(left.amount), abs(right.amount), Decimal("0"))
    pct_tolerance = max_amount * Decimal(str(validated.pending_amount_tolerance_pct))
    amount_tolerance = max(validated.pending_amount_tolerance_abs, pct_tolerance)
    within_date_window = day_delta <= validated.pending_posted_window_days
    within_amount_tolerance = amount_delta <= amount_tolerance
    # Intentionally conservative: _jaccard_similarity returns 0.0 when either side is empty,
    # so pairs with one/both empty payees fail merchant_pass and cannot match on date/amount alone.
    merchant_pass = payee_exact or payee_similarity >= _PENDING_POSTED_SIMILARITY_FLOOR

    return _PendingPostedMatchEvaluation(
        is_pending_posted_pair=True,
        is_match=within_date_window and within_amount_tolerance and merchant_pass,
        date_delta_days=day_delta,
        amount_delta=amount_delta,
        amount_tolerance=amount_tolerance,
        payee_similarity=payee_similarity,
        payee_exact=payee_exact,
    )


def _transaction_snapshot(value: _TransactionView) -> dict[str, object]:
    return {
        "id": value.id,
        "account_id": value.account_id,
        "posted_date": value.posted_date.isoformat(),
        "amount": str(value.amount),
        "currency": value.currency,
        "source_kind": value.source_kind,
        "pending_status": value.pending_status,
        "merchant_name": value.merchant_name or None,
        "original_statement": value.original_statement or None,
    }


def _score_breakdown_payload(score_breakdown: DedupeScoreBreakdown) -> dict[str, object]:
    return {
        "amount_factor": score_breakdown.amount_factor,
        "date_factor": score_breakdown.date_factor,
        "merchant_payee_factor": score_breakdown.merchant_payee_factor,
        "statement_factor": score_breakdown.statement_factor,
        "source_kind_factor": score_breakdown.source_kind_factor,
        "total_score": score_breakdown.total_score,
        "details": score_breakdown.details,
    }


def _candidate_reason_payload(
    *,
    match_type: str,
    score_breakdown: DedupeScoreBreakdown,
    left: _TransactionView,
    right: _TransactionView,
    policy: dict[str, object],
) -> dict[str, object]:
    return {
        "match_type": match_type,
        "policy": policy,
        "score_breakdown": _score_breakdown_payload(score_breakdown),
        "txn_a_snapshot": _transaction_snapshot(left),
        "txn_b_snapshot": _transaction_snapshot(right),
    }


def _fetch_transactions(validated: _ValidatedRequest, session: Session) -> list[_TransactionView]:
    stmt = (
        select(Transaction, Merchant.canonical_name)
        .outerjoin(Merchant, Merchant.id == Transaction.merchant_id)
        .order_by(Transaction.posted_date.asc(), Transaction.id.asc())
        .limit(validated.limit)
    )
    if validated.scope_transaction_ids:
        stmt = stmt.where(Transaction.id.in_(validated.scope_transaction_ids))
    if not validated.include_pending:
        stmt = stmt.where(Transaction.pending_status == "posted")

    rows = session.execute(stmt).all()
    transactions: list[_TransactionView] = []
    for transaction, merchant_name in rows:
        merchant_name = merchant_name or ""
        original_statement = transaction.original_statement or ""
        payee_source = merchant_name or original_statement
        transactions.append(
            _TransactionView(
                id=transaction.id,
                account_id=transaction.account_id,
                posted_date=transaction.posted_date,
                amount=transaction.amount,
                currency=transaction.currency,
                source_kind=transaction.source_kind,
                pending_status=transaction.pending_status,
                original_statement=original_statement,
                merchant_name=merchant_name,
                normalized_payee=_normalize_text(payee_source),
                normalized_statement=_normalize_text(original_statement),
            )
        )
    return transactions


def _upsert_candidate(
    *,
    pair_key: tuple[str, str],
    score: float,
    decision: str | None,
    reason_json: dict[str, object],
    clear_decision_when_none: bool,
    session: Session,
) -> tuple[DedupeCandidate, bool, bool]:
    now = utcnow()
    proposed_candidate_id = str(uuid4())
    decided_at = now if decision is not None else None

    insert_stmt = sqlite_insert(DedupeCandidate).values(
        id=proposed_candidate_id,
        txn_a_id=pair_key[0],
        txn_b_id=pair_key[1],
        score=score,
        decision=decision,
        reason_json=reason_json,
        created_at=now,
        decided_at=decided_at,
    )
    incoming_decision = insert_stmt.excluded.decision
    if clear_decision_when_none:
        decision_update_expr = incoming_decision
        decided_at_update_expr = case(
            (incoming_decision.is_(None), null()),
            (
                or_(
                    DedupeCandidate.decision.is_distinct_from(incoming_decision),
                    DedupeCandidate.decided_at.is_(None),
                ),
                now,
            ),
            else_=DedupeCandidate.decided_at,
        )
    else:
        decision_update_expr = case(
            (incoming_decision.is_(None), DedupeCandidate.decision),
            else_=incoming_decision,
        )
        decided_at_update_expr = case(
            (incoming_decision.is_(None), DedupeCandidate.decided_at),
            (
                or_(
                    DedupeCandidate.decision.is_distinct_from(incoming_decision),
                    DedupeCandidate.decided_at.is_(None),
                ),
                now,
            ),
            else_=DedupeCandidate.decided_at,
        )
    update_conditions = [
        DedupeCandidate.score != insert_stmt.excluded.score,
        DedupeCandidate.reason_json != insert_stmt.excluded.reason_json,
        and_(
            incoming_decision.is_not(None),
            DedupeCandidate.decision.is_distinct_from(incoming_decision),
        ),
        and_(
            incoming_decision.is_not(None),
            DedupeCandidate.decided_at.is_(None),
        ),
    ]
    if clear_decision_when_none:
        update_conditions.append(
            and_(
                incoming_decision.is_(None),
                or_(
                    DedupeCandidate.decision.is_not(None),
                    DedupeCandidate.decided_at.is_not(None),
                ),
            )
        )

    upsert_stmt = (
        insert_stmt.on_conflict_do_update(
            index_elements=[DedupeCandidate.txn_a_id, DedupeCandidate.txn_b_id],
            set_={
                "score": insert_stmt.excluded.score,
                "decision": decision_update_expr,
                "reason_json": insert_stmt.excluded.reason_json,
                "decided_at": decided_at_update_expr,
            },
            where=or_(*update_conditions),
        )
        .returning(DedupeCandidate.id)
    )
    candidate_id = session.scalar(upsert_stmt)
    if candidate_id is None:
        candidate_id = session.scalar(
            select(DedupeCandidate.id)
            .where(
                DedupeCandidate.txn_a_id == pair_key[0],
                DedupeCandidate.txn_b_id == pair_key[1],
            )
            .limit(1)
        )
        if candidate_id is None:
            raise _DedupePersistenceError.unresolved_pair(pair_key)
        candidate = session.get(DedupeCandidate, candidate_id, populate_existing=True)
        if candidate is None:
            raise _DedupePersistenceError.unresolved_candidate(candidate_id)
        return candidate, True, False

    candidate = session.get(DedupeCandidate, candidate_id, populate_existing=True)
    if candidate is None:
        raise _DedupePersistenceError.unresolved_candidate(candidate_id)
    return candidate, False, candidate.id == proposed_candidate_id


def _candidate_state_payload(candidate: DedupeCandidate) -> dict[str, object]:
    return {
        "score": candidate.score,
        "decision": candidate.decision,
        "reason_json": candidate.reason_json,
        "decided_at": candidate.decided_at.isoformat() if candidate.decided_at else None,
    }


def _record_candidate_event(
    *,
    candidate_id: str,
    event_type: str,
    old_value_json: dict[str, object] | None,
    new_value_json: dict[str, object] | None,
    actor: str,
    reason: str,
    session: Session,
) -> None:
    session.add(
        DedupeCandidateEvent(
            id=str(uuid4()),
            dedupe_candidate_id=candidate_id,
            event_type=event_type,
            old_value_json=old_value_json,
            new_value_json=new_value_json,
            actor=actor,
            reason=reason,
            created_at=utcnow(),
        )
    )


def _get_active_review_item(
    *,
    candidate_id: str,
    active_reviews_by_candidate_id: dict[str, ReviewItem | None],
    session: Session,
) -> ReviewItem | None:
    if candidate_id in active_reviews_by_candidate_id:
        return active_reviews_by_candidate_id[candidate_id]

    review_item = session.scalar(
        select(ReviewItem)
        .where(
            ReviewItem.ref_table == _SOFT_REVIEW_REF_TABLE,
            ReviewItem.ref_id == candidate_id,
            ReviewItem.source == ReviewSource.DEDUPE.value,
            ReviewItem.status.in_(_ACTIVE_REVIEW_STATUSES),
        )
        .order_by(ReviewItem.created_at.asc(), ReviewItem.id.asc())
        .limit(1)
    )
    active_reviews_by_candidate_id[candidate_id] = review_item
    return review_item


def _close_active_review_item_for_autolink(
    *,
    candidate_id: str,
    actor: str,
    reason: str,
    active_reviews_by_candidate_id: dict[str, ReviewItem | None],
    session: Session,
) -> None:
    review_item = _get_active_review_item(
        candidate_id=candidate_id,
        active_reviews_by_candidate_id=active_reviews_by_candidate_id,
        session=session,
    )
    if review_item is None:
        return

    now = utcnow()
    payload_json = dict(review_item.payload_json) if isinstance(review_item.payload_json, dict) else {}
    payload_json["resolution"] = {
        "status": "auto_resolved_duplicate",
        "actor": actor,
        "reason": reason,
        "resolved_at": now.isoformat(),
    }
    review_item.payload_json = payload_json
    review_item.status = ReviewItemStatus.RESOLVED.value
    review_item.resolved_at = now
    active_reviews_by_candidate_id[candidate_id] = None


def _ensure_soft_review_item(
    *,
    candidate: DedupeCandidate,
    score: float,
    left: _TransactionView,
    right: _TransactionView,
    score_breakdown: DedupeScoreBreakdown,
    reason_code: str,
    reason_codes: list[str],
    policy: dict[str, object],
    actor: str,
    reason: str,
    active_reviews_by_candidate_id: dict[str, ReviewItem | None],
    session: Session,
) -> ReviewItem:
    now = utcnow()
    payload = {
        "suggestion": {
            "kind": _SOFT_SUGGESTION_KIND,
            "dedupe_candidate_id": candidate.id,
            "decision": _DUPLICATE_DECISION,
            "confidence": score,
            "reason_codes": reason_codes,
            "generated_at": now.isoformat(),
            "score_breakdown": _score_breakdown_payload(score_breakdown),
            "policy": policy,
            "actor": actor,
            "reason": reason,
        },
        "candidate": {
            "txn_a_id": candidate.txn_a_id,
            "txn_b_id": candidate.txn_b_id,
            "txn_a_snapshot": _transaction_snapshot(left),
            "txn_b_snapshot": _transaction_snapshot(right),
        },
    }

    existing_review = _get_active_review_item(
        candidate_id=candidate.id,
        active_reviews_by_candidate_id=active_reviews_by_candidate_id,
        session=session,
    )
    if existing_review is not None:
        existing_review.reason_code = reason_code
        existing_review.confidence = score
        existing_review.payload_json = payload
        return existing_review

    review_item_id = session.scalar(
        sqlite_insert(ReviewItem)
        .values(
            id=str(uuid4()),
            item_type=_SOFT_REVIEW_ITEM_TYPE,
            ref_table=_SOFT_REVIEW_REF_TABLE,
            ref_id=candidate.id,
            reason_code=reason_code,
            confidence=score,
            status=ReviewItemStatus.TO_REVIEW.value,
            source=ReviewSource.DEDUPE.value,
            assigned_to=None,
            payload_json=payload,
            created_at=now,
            resolved_at=None,
        )
        .on_conflict_do_nothing(
            index_elements=[
                ReviewItem.ref_table,
                ReviewItem.ref_id,
                ReviewItem.item_type,
                ReviewItem.source,
            ],
            index_where=and_(
                ReviewItem.ref_table == _SOFT_REVIEW_REF_TABLE,
                ReviewItem.item_type == _SOFT_REVIEW_ITEM_TYPE,
                ReviewItem.source == ReviewSource.DEDUPE.value,
                ReviewItem.status.in_(_ACTIVE_REVIEW_STATUSES),
            ),
        )
        .returning(ReviewItem.id)
    )

    if review_item_id is None:
        active_reviews_by_candidate_id.pop(candidate.id, None)
        existing_review = _get_active_review_item(
            candidate_id=candidate.id,
            active_reviews_by_candidate_id=active_reviews_by_candidate_id,
            session=session,
        )
        if existing_review is None:
            raise _DedupePersistenceError.unresolved_active_review(candidate.id)
        existing_review.reason_code = reason_code
        existing_review.confidence = score
        existing_review.payload_json = payload
        return existing_review

    review_item = session.get(ReviewItem, review_item_id)
    if review_item is None:
        raise _DedupePersistenceError.unresolved_review_item(review_item_id)
    active_reviews_by_candidate_id[candidate.id] = review_item
    return review_item


def txn_dedupe_match(request: TxnDedupeMatchRequest, session: Session) -> TxnDedupeMatchResult:
    """Generate deterministic hard/soft dedupe candidates and review hooks."""

    validated = _validate_request(request)
    # Ensure in-session pending writes participate in idempotent candidate lookups.
    session.flush()
    transactions = _fetch_transactions(validated, session)
    if len(transactions) < 2:
        return TxnDedupeMatchResult(
            hard_auto_linked=0,
            soft_queued=0,
            soft_auto_linked=0,
            skipped_existing=0,
            candidates=[],
        )

    active_reviews_by_candidate_id: dict[str, ReviewItem | None] = {}
    transaction_ids = [transaction.id for transaction in transactions]
    existing_candidates_by_pair: dict[tuple[str, str], DedupeCandidate] = {}
    if transaction_ids:
        existing_candidates = session.scalars(
            select(DedupeCandidate).where(
                DedupeCandidate.txn_a_id.in_(transaction_ids),
                DedupeCandidate.txn_b_id.in_(transaction_ids),
            )
        ).all()
        existing_candidates_by_pair = {
            _pair_key(candidate.txn_a_id, candidate.txn_b_id): candidate
            for candidate in existing_candidates
        }

    hard_auto_linked = 0
    soft_queued = 0
    soft_auto_linked = 0
    skipped_existing = 0
    results: list[DedupeCandidateResult] = []

    processed_pairs: set[tuple[str, str]] = set()

    max_candidate_window_days = max(
        validated.soft_candidate_window_days,
        validated.hard_date_window_days,
    )
    if validated.include_pending:
        max_candidate_window_days = max(
            max_candidate_window_days,
            validated.pending_posted_window_days,
        )
    for idx, left in enumerate(transactions):
        for right in transactions[idx + 1 :]:
            day_delta = abs((left.posted_date - right.posted_date).days)
            if day_delta > max_candidate_window_days and right.posted_date >= left.posted_date:
                break

            if left.account_id != right.account_id:
                continue
            if left.currency != right.currency:
                continue

            pair = _pair_key(left.id, right.id)
            if pair in processed_pairs:
                continue
            processed_pairs.add(pair)
            if pair[0] == left.id:
                ordered_left = left
                ordered_right = right
            else:
                ordered_left = right
                ordered_right = left

            pending_posted_eval = _evaluate_pending_posted_match(left, right, validated=validated)
            if pending_posted_eval.is_match:
                amount_denominator = max(pending_posted_eval.amount_tolerance, Decimal("1"))
                amount_factor = max(
                    0.0,
                    1.0 - float(pending_posted_eval.amount_delta / amount_denominator),
                )
                if validated.pending_posted_window_days == 0:
                    date_factor = 1.0 if pending_posted_eval.date_delta_days == 0 else 0.0
                else:
                    date_factor = max(
                        0.0,
                        1.0 - (pending_posted_eval.date_delta_days / validated.pending_posted_window_days),
                    )
                merchant_payee_factor = (
                    1.0 if pending_posted_eval.payee_exact else pending_posted_eval.payee_similarity
                )
                score_breakdown = DedupeScoreBreakdown(
                    amount_factor=amount_factor,
                    date_factor=date_factor,
                    merchant_payee_factor=merchant_payee_factor,
                    statement_factor=_jaccard_similarity(
                        ordered_left.normalized_statement,
                        ordered_right.normalized_statement,
                    ),
                    source_kind_factor=1.0 if ordered_left.source_kind == ordered_right.source_kind else 0.0,
                    # Pending->posted hard matches are deterministic; sub-factors are audit context only.
                    total_score=1.0,
                    details={
                        "hard_match": True,
                        "pending_posted_link": True,
                        "subfactors_informational": True,
                        "date_delta_days": pending_posted_eval.date_delta_days,
                        "amount_delta": str(pending_posted_eval.amount_delta),
                        "amount_tolerance": str(pending_posted_eval.amount_tolerance),
                        "payee_similarity": pending_posted_eval.payee_similarity,
                    },
                )
                classification = "hard"
                score = 1.0
                decision = _DUPLICATE_DECISION
            elif _is_hard_match(left, right, hard_date_window_days=validated.hard_date_window_days):
                score_breakdown = DedupeScoreBreakdown(
                    amount_factor=1.0,
                    date_factor=1.0,
                    merchant_payee_factor=1.0,
                    statement_factor=_jaccard_similarity(
                        ordered_left.normalized_statement,
                        ordered_right.normalized_statement,
                    ),
                    source_kind_factor=1.0 if ordered_left.source_kind == ordered_right.source_kind else 0.0,
                    total_score=1.0,
                    details={
                        "hard_match": True,
                        "date_delta_days": day_delta,
                    },
                )
                classification = "hard"
                score = 1.0
                decision = _DUPLICATE_DECISION
            else:
                if day_delta > validated.soft_candidate_window_days:
                    continue
                score_breakdown = _compute_soft_score(
                    ordered_left,
                    ordered_right,
                    soft_candidate_window_days=validated.soft_candidate_window_days,
                )
                score = score_breakdown.total_score
                if score < validated.soft_review_threshold:
                    continue

                classification = "soft"
                if score >= validated.soft_autolink_threshold:
                    decision = _DUPLICATE_DECISION
                else:
                    decision = None

            # Cross-source review-only policy applies uniformly to any candidate that
            # would otherwise be auto-linked as duplicate, including pending->posted matches.
            is_cross_source = ordered_left.source_kind != ordered_right.source_kind
            cross_source_review_only_applied = bool(
                is_cross_source
                and validated.cross_source_review_only
                and decision == _DUPLICATE_DECISION
            )
            if cross_source_review_only_applied:
                decision = None

            policy_flags = {
                "pending_posted_link": pending_posted_eval.is_match,
                "cross_source_review_only_applied": cross_source_review_only_applied,
            }
            review_reason_code = (
                _CROSS_SOURCE_REVIEW_REASON
                if cross_source_review_only_applied
                else _SOFT_REVIEW_REASON
            )
            reason_codes = [review_reason_code]
            policy_context: dict[str, object] = {
                "cross_source_pair": is_cross_source,
                "cross_source_review_only": validated.cross_source_review_only,
                "cross_source_review_only_applied": cross_source_review_only_applied,
                "pending_posted_link": pending_posted_eval.is_match,
            }
            if pending_posted_eval.is_pending_posted_pair:
                policy_context["pending_posted"] = {
                    "matched": pending_posted_eval.is_match,
                    "date_delta_days": pending_posted_eval.date_delta_days,
                    "amount_delta": str(pending_posted_eval.amount_delta),
                    "amount_tolerance": str(pending_posted_eval.amount_tolerance),
                    "payee_similarity": pending_posted_eval.payee_similarity,
                    "payee_exact": pending_posted_eval.payee_exact,
                }

            reason_json = _candidate_reason_payload(
                match_type=classification,
                score_breakdown=score_breakdown,
                left=ordered_left,
                right=ordered_right,
                policy=policy_context,
            )
            existing_candidate = existing_candidates_by_pair.get(pair)
            existing_state = (
                _candidate_state_payload(existing_candidate)
                if existing_candidate is not None
                else None
            )
            existing_decision = existing_candidate.decision if existing_candidate is not None else None

            candidate, was_unchanged, was_created = _upsert_candidate(
                pair_key=pair,
                score=score,
                decision=decision,
                reason_json=reason_json,
                clear_decision_when_none=cross_source_review_only_applied,
                session=session,
            )
            if was_unchanged:
                skipped_existing += 1

            candidate_state = _candidate_state_payload(candidate)
            if was_created:
                _record_candidate_event(
                    candidate_id=candidate.id,
                    event_type=_CANDIDATE_CREATED_EVENT_TYPE,
                    old_value_json=None,
                    new_value_json=candidate_state,
                    actor=validated.actor,
                    reason=validated.reason,
                    session=session,
                )
            elif existing_state is not None and existing_state != candidate_state:
                _record_candidate_event(
                    candidate_id=candidate.id,
                    event_type=_CANDIDATE_UPDATED_EVENT_TYPE,
                    old_value_json=existing_state,
                    new_value_json=candidate_state,
                    actor=validated.actor,
                    reason=validated.reason,
                    session=session,
                )
            if existing_state is not None and existing_decision != candidate.decision:
                _record_candidate_event(
                    candidate_id=candidate.id,
                    event_type=_CANDIDATE_DECISION_CHANGED_EVENT_TYPE,
                    old_value_json={"decision": existing_decision},
                    new_value_json={"decision": candidate.decision},
                    actor=validated.actor,
                    reason=validated.reason,
                    session=session,
                )

            effective_decision = candidate.decision
            queued_review_item_id: str | None = None
            if effective_decision == _DUPLICATE_DECISION:
                _close_active_review_item_for_autolink(
                    candidate_id=candidate.id,
                    actor=validated.actor,
                    reason=validated.reason,
                    active_reviews_by_candidate_id=active_reviews_by_candidate_id,
                    session=session,
                )
                if classification == "hard":
                    hard_auto_linked += 1
                else:
                    soft_auto_linked += 1
            else:
                review_item = _ensure_soft_review_item(
                    candidate=candidate,
                    score=score,
                    left=ordered_left,
                    right=ordered_right,
                    score_breakdown=score_breakdown,
                    reason_code=review_reason_code,
                    reason_codes=reason_codes,
                    policy=policy_context,
                    actor=validated.actor,
                    reason=validated.reason,
                    active_reviews_by_candidate_id=active_reviews_by_candidate_id,
                    session=session,
                )
                queued_review_item_id = review_item.id
                soft_queued += 1

            results.append(
                DedupeCandidateResult(
                    dedupe_candidate_id=candidate.id,
                    txn_a_id=candidate.txn_a_id,
                    txn_b_id=candidate.txn_b_id,
                    score=score,
                    classification=classification,
                    decision=candidate.decision,
                    queued_review_item_id=queued_review_item_id,
                    score_breakdown=score_breakdown,
                    policy_flags=policy_flags,
                )
            )

    return TxnDedupeMatchResult(
        hard_auto_linked=hard_auto_linked,
        soft_queued=soft_queued,
        soft_auto_linked=soft_auto_linked,
        skipped_existing=skipped_existing,
        candidates=results,
    )
