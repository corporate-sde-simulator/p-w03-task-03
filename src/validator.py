"""
Data Validator — validates records against a schema before loading.

YOU MUST IMPLEMENT the methods marked with TODO below.
The Pipeline in pipeline.py already calls these methods — your job is to make them work.

Schema format example:
{
    "name": {"type": "str", "required": True},
    "age": {"type": "int", "required": True, "min": 0, "max": 150},
    "email": {"type": "str", "required": True, "pattern": "email"},
    "score": {"type": "float", "required": False, "min": 0.0, "max": 100.0}
}
"""

from typing import Dict, List, Callable, Optional
import re


class DataValidator:
    """Validates data records against a defined schema."""

    def __init__(self, schema: Dict):
        self.schema = schema
        self.custom_rules: List[Dict] = []
        self.quarantine: List[Dict] = []
        self._stats = {
            'total': 0,
            'passed': 0,
            'failed': 0,
            'failure_breakdown': {}  # e.g., {'missing_field': 3, 'wrong_type': 1}
        }

    def validate_record(self, record: Dict) -> Dict:
        """
        Validate a single record against the schema.
        
        Returns: {
            'valid': True/False,
            'errors': [  # list of error dicts
                {'field': 'age', 'error_type': 'wrong_type', 'message': 'Expected int, got str'},
                ...
            ]
        }
        
        Must check:
        1. Required fields exist (error_type: 'missing_field')
        2. Field types match schema (error_type: 'wrong_type')
        3. Numeric fields within min/max range (error_type: 'out_of_range')
        4. Custom rules pass (error_type: 'custom_rule')
        """
        self._stats['total'] += 1
        errors = []

        # TODO: Check required fields
        # For each field in self.schema where required=True,
        # check if the field exists in the record.
        # If missing, append: {'field': name, 'error_type': 'missing_field',
        #                       'message': f'Required field {name} is missing'}

        # TODO: Check field types
        # For each field present in both record and schema,
        # check if type matches. Use these mappings:
        # 'str' -> str, 'int' -> int, 'float' -> (int, float), 'bool' -> bool
        # If wrong type, append: {'field': name, 'error_type': 'wrong_type',
        #                          'message': f'Expected {expected}, got {actual}'}

        # TODO: Check numeric ranges
        # For int/float fields that have 'min' or 'max' in schema,
        # check if value is within range.
        # If out of range, append: {'field': name, 'error_type': 'out_of_range',
        #                            'message': f'{name} value {value} outside range [{min}, {max}]'}

        # TODO: Apply custom rules
        # For each rule in self.custom_rules, call rule['check_fn'](record)
        # If it returns False, append: {'field': rule['name'], 'error_type': 'custom_rule',
        #                                'message': rule['error_message']}

        valid = len(errors) == 0

        if valid:
            self._stats['passed'] += 1
        else:
            self._stats['failed'] += 1
            for error in errors:
                error_type = error['error_type']
                self._stats['failure_breakdown'][error_type] = \
                    self._stats['failure_breakdown'].get(error_type, 0) + 1
            # Add to quarantine
            self.quarantine.append({'record': record, 'errors': errors})

        return {'valid': valid, 'errors': errors}

    def add_custom_rule(self, name: str, check_fn: Callable[[Dict], bool],
                        error_message: str):
        """
        Add a custom validation rule.
        
        TODO: Implement this method.
        Store the rule so validate_record() can apply it.
        Each rule should have: name, check_fn, error_message
        """
        pass  # TODO: Store the custom rule in self.custom_rules

    def get_validation_report(self) -> Dict:
        """
        Return validation statistics.
        
        TODO: Implement this method.
        Return self._stats which tracks total, passed, failed, and failure_breakdown.
        Also include quarantine_count.
        """
        pass  # TODO: Return the stats dict with quarantine count added

    def get_quarantined_records(self) -> List[Dict]:
        """Return all quarantined (invalid) records."""
        return self.quarantine.copy()
