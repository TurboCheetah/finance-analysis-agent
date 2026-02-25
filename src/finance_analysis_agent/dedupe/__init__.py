"""Transaction dedupe service exports."""

from finance_analysis_agent.dedupe.service import txn_dedupe_match
from finance_analysis_agent.dedupe.types import (
    DedupeCandidateResult,
    DedupeScoreBreakdown,
    TxnDedupeMatchRequest,
    TxnDedupeMatchResult,
)

__all__ = [
    "DedupeCandidateResult",
    "DedupeScoreBreakdown",
    "TxnDedupeMatchRequest",
    "TxnDedupeMatchResult",
    "txn_dedupe_match",
]
