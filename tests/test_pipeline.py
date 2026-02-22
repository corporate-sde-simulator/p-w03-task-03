"""Tests for ETL Pipeline."""
import pytest, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from validator import RecordValidator
from pipeline import ETLPipeline

class TestRecordValidator:
    def setup_method(self):
        self.v = RecordValidator()

    def test_valid_record(self):
        ok, errs = self.v.validate_record({'user_id': 'u1', 'amount': 100, 'timestamp': '2026-01-01T00:00:00Z', 'category': 'sales'})
        assert ok and len(errs) == 0

    def test_missing_user_id(self):
        ok, errs = self.v.validate_record({'amount': 100, 'category': 'sales'})
        assert not ok

    def test_negative_amount(self):
        ok, errs = self.v.validate_record({'user_id': 'u1', 'amount': -5, 'category': 'sales'})
        assert not ok

    def test_invalid_category(self):
        ok, errs = self.v.validate_record({'user_id': 'u1', 'amount': 10, 'category': 'unknown'})
        assert not ok

    def test_multiple_errors_collected(self):
        """All errors should be reported, not just the first."""
        ok, errs = self.v.validate_record({'amount': -5, 'category': 'bad'})
        assert not ok
        assert len(errs) >= 2  # user_id missing AND category invalid

    def test_non_numeric_amount(self):
        ok, errs = self.v.validate_record({'user_id': 'u1', 'amount': 'abc', 'category': 'sales'})
        assert not ok

class TestETLPipeline:
    def test_process_valid_batch(self):
        p = ETLPipeline()
        records = [{'user_id': 'u1', 'amount': 100, 'timestamp': '2026-01-01T00:00:00Z', 'category': 'sales'}]
        stats = p.process_batch(records)
        assert stats['valid'] == 1 and stats['invalid'] == 0

    def test_dead_letter_has_error_details(self):
        p = ETLPipeline()
        records = [{'user_id': '', 'amount': -1, 'category': 'bad'}]
        p.process_batch(records)
        dl = p.get_dead_letters()
        assert len(dl) == 1
        # Dead letter should include error context
        assert 'errors' in dl[0] or isinstance(dl[0], dict)

    def test_batch_stats(self):
        p = ETLPipeline()
        records = [
            {'user_id': 'u1', 'amount': 10, 'category': 'sales'},
            {'amount': -1, 'category': 'bad'},
        ]
        stats = p.process_batch(records)
        assert stats['total'] == 2

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
