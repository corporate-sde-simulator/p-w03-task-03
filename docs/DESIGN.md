# ADR-027: ETL Validation Strategy
**Date:** Â· **Status:** Accepted Â· **Authors:** Priya Menon, Raj Kapoor

## Decision
Use **inline validation** with dead-letter routing for invalid records rather than pre-filtering.

## Rationale
- Invalid records are logged with context for partner debugging
- Valid records aren't delayed by batch validation
- Dead-letter queue enables retry and manual review

## Consequences
- Every record incurs validation overhead
- Dead-letter queue needs monitoring and alerting
- Must collect ALL errors per record, not just the first one
