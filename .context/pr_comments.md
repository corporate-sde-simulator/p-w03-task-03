# PR #435 Review — ETL Validation Pipeline (by Raj Kapoor)
## Reviewer: Priya Menon — Feb 21, 2026
---
### `validator.py`
> **Line 32** — `validate_record`: Returns on first error instead of collecting all errors. A record with 3 bad fields only reports the first issue.
> **Line 45** — `validate_amount`: Uses `int()` coercion instead of checking type. Strings like "abc" cause uncaught ValueError instead of clean validation error.

### `pipeline.py`
> **Line 28** — Dead-letter routing: Records are added without the error details. Need to include which fields failed and why.

---
**Raj Kapoor** — Feb 22, 2026
> The early-return thing was intentional for performance but you're right — we need all errors for debugging.
