# Meeting Notes — Sprint 25 Planning
**Date:** Feb 24, 2026  
**Attendees:** Priya (Data Lead), Raj, Intern

---
## ETL Pipeline Validation
- **Priya:** We had 12K corrupt records in the warehouse last week. @Intern, PLATFORM-2875 is yours — fix Raj's validation pipeline.
- **Raj:** The validator catches some errors but misses type mismatches. Also, when a record has multiple invalid fields, only the first error is reported.

## Action Items
- [ ] @Intern — Fix ETL validation pipeline (PLATFORM-2875)
- [ ] @Raj — Code review
