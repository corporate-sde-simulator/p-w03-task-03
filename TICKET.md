# PLATFORM-2875: Build data validation layer for ETL pipeline

**Status:** In Progress Â· **Priority:** High
**Sprint:** Sprint 25 Â· **Story Points:** 5
**Reporter:** Raj Kapoor (Data Engineering Lead) Â· **Assignee:** You (Intern)
**Created:** Â· **Due:** End of sprint (Friday)
**Labels:** `backend`, `python`, `data-pipeline`, `feature`
**Epic:** PLATFORM-2870 (Data Platform v3)
**Task Type:** ðŸš€ Feature Ship

---

## Description

The ETL pipeline currently reads data from source and loads it into the data warehouse, but has **no validation layer**. Invalid records (missing fields, wrong types, out-of-range values) make it into production tables and break downstream dashboards.

A clean, working `Pipeline` class exists in `src/pipeline.py`. Your job is to **build the data validation layer** by implementing the TODO items in `src/validator.py`. The pipeline already calls the validator â€” you just need to implement the validation logic.

## Requirements

- Validate each record against a schema (required fields, data types, value ranges)
- Categorize validation failures (missing_field, wrong_type, out_of_range, format_error)
- Support custom validation rules via callback functions
- Track validation statistics (total, passed, failed, failure breakdown)
- Invalid records go to a quarantine list (not dropped silently)

## Acceptance Criteria

- [ ] `validate_record()` checks all required fields exist
- [ ] `validate_record()` checks field types match schema
- [ ] `validate_record()` checks numeric ranges where specified
- [ ] `add_custom_rule()` registers and applies custom validators
- [ ] `get_validation_report()` returns complete stats
- [ ] Invalid records added to quarantine with failure reason
- [ ] All unit tests pass

## Design Notes

See `docs/DESIGN.md` for the ETL architecture.
The `Pipeline` class in `pipeline.py` is working and tested â€” don't modify it.
