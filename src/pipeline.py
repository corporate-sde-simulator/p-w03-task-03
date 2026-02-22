"""
ETL Pipeline — reads data from source, transforms, and loads to destination.

This module is COMPLETE and WORKING. Do NOT modify this file.
Your task is to implement the Validator in validator.py.
The pipeline already calls the validator — you just need to implement its methods.
"""

from typing import Dict, List, Optional, Callable
from validator import DataValidator


class Pipeline:
    """Extract-Transform-Load pipeline with pluggable validation."""

    def __init__(self, name: str, schema: Dict):
        self.name = name
        self.schema = schema
        self.validator = DataValidator(schema)
        self.processed_records: List[Dict] = []
        self.transform_functions: List[Callable] = []
        self._stats = {'extracted': 0, 'transformed': 0, 'loaded': 0, 'rejected': 0}

    def add_transform(self, transform_fn: Callable[[Dict], Dict]):
        """Add a transformation function to the pipeline."""
        self.transform_functions.append(transform_fn)

    def run(self, source_data: List[Dict]) -> Dict:
        """
        Run the full ETL pipeline on source data.
        Returns: { loaded: [...valid records...], rejected: [...invalid records...], stats: {...} }
        """
        loaded = []
        rejected = []

        for record in source_data:
            self._stats['extracted'] += 1

            # VALIDATE: Check record against schema
            validation_result = self.validator.validate_record(record)
            if not validation_result['valid']:
                self._stats['rejected'] += 1
                rejected.append({
                    'record': record,
                    'errors': validation_result['errors']
                })
                continue

            # TRANSFORM: Apply all transform functions
            transformed = record.copy()
            for transform_fn in self.transform_functions:
                transformed = transform_fn(transformed)
            self._stats['transformed'] += 1

            # LOAD: Add to processed records
            self.processed_records.append(transformed)
            loaded.append(transformed)
            self._stats['loaded'] += 1

        return {
            'loaded': loaded,
            'rejected': rejected,
            'stats': self._stats.copy(),
            'validation_report': self.validator.get_validation_report()
        }

    def get_stats(self) -> Dict:
        """Return pipeline execution statistics."""
        return self._stats.copy()

    def reset(self):
        """Reset pipeline state for a new run."""
        self.processed_records = []
        self._stats = {'extracted': 0, 'transformed': 0, 'loaded': 0, 'rejected': 0}
        self.validator = DataValidator(self.schema)
