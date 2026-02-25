# PDF Quality Fixtures

These fixtures provide deterministic row-level quality checks for TUR-36.

Each `*.json` fixture defines:
- `template_hint`: parsing profile key.
- `metadata`: request metadata (`statement_year`, `currency`, optional `issuer`).
- `text_pages`: text-page payload injected into the layered parser.
- `expected_rows`: expected parsed canonical rows for precision/recall scoring.

Precision/recall is calculated against parsed rows only (not parse-error rows).
