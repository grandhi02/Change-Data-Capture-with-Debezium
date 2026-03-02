# validators.py

import re
import time
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[Dict]
    warnings: List[Dict]
    record_key: Optional[str] = None
    table: Optional[str] = None


class BaseValidator:
    """Base class for all validators."""

    def validate(self, event: Dict) -> List[Dict]:
        raise NotImplementedError


class NullCheckValidator(BaseValidator):
    """Check that required fields are not null."""

    def __init__(self, required_fields: List[str]):
        self.required_fields = required_fields

    def validate(self, payload: Dict) -> List[Dict]:
        errors = []
        for field in self.required_fields:
            value = payload.get(field)
            if value is None:
                errors.append({
                    "check": "null_check",
                    "field": field,
                    "severity": ValidationSeverity.ERROR.value,
                    "message": f"Required field '{field}' is null",
                })
        return errors


class TypeCheckValidator(BaseValidator):
    """Check that fields have the expected types."""

    TYPE_MAP = {
        "string": str,
        "int": int,
        "float": (int, float),
        "bool": bool,
    }

    def __init__(self, field_types: Dict[str, str]):
        self.field_types = field_types

    def validate(self, payload: Dict) -> List[Dict]:
        errors = []
        for field, expected_type_name in self.field_types.items():
            value = payload.get(field)
            if value is None:
                continue  # null checks handled separately
            expected_type = self.TYPE_MAP.get(expected_type_name)
            if expected_type and not isinstance(value, expected_type):
                errors.append({
                    "check": "type_check",
                    "field": field,
                    "severity": ValidationSeverity.ERROR.value,
                    "message": f"Field '{field}' expected {expected_type_name}, got {type(value).__name__}",
                })
        return errors


class RangeCheckValidator(BaseValidator):
    """Check that numeric fields are within expected ranges."""

    def __init__(self, field_ranges: Dict[str, Tuple[float, float]]):
        self.field_ranges = field_ranges

    def validate(self, payload: Dict) -> List[Dict]:
        errors = []
        for field, (min_val, max_val) in self.field_ranges.items():
            value = payload.get(field)
            if value is None:
                continue
            try:
                numeric_value = float(value)
                if numeric_value < min_val or numeric_value > max_val:
                    errors.append({
                        "check": "range_check",
                        "field": field,
                        "severity": ValidationSeverity.ERROR.value,
                        "message": f"Field '{field}' value {numeric_value} outside range [{min_val}, {max_val}]",
                    })
            except (ValueError, TypeError):
                errors.append({
                    "check": "range_check",
                    "field": field,
                    "severity": ValidationSeverity.ERROR.value,
                    "message": f"Field '{field}' is not numeric",
                })
        return errors


class AllowedValuesValidator(BaseValidator):
    """Check that fields contain only allowed values."""

    def __init__(self, field_allowed: Dict[str, List]):
        self.field_allowed = field_allowed

    def validate(self, payload: Dict) -> List[Dict]:
        errors = []
        for field, allowed in self.field_allowed.items():
            value = payload.get(field)
            if value is None:
                continue
            if value not in allowed:
                errors.append({
                    "check": "allowed_values",
                    "field": field,
                    "severity": ValidationSeverity.ERROR.value,
                    "message": f"Field '{field}' value '{value}' not in allowed values: {allowed}",
                })
        return errors


class EmailValidator(BaseValidator):
    """Validate email format."""

    EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")

    def __init__(self, email_fields: List[str]):
        self.email_fields = email_fields

    def validate(self, payload: Dict) -> List[Dict]:
        errors = []
        for field in self.email_fields:
            value = payload.get(field)
            if value is None:
                continue
            if not self.EMAIL_REGEX.match(str(value)):
                errors.append({
                    "check": "email_format",
                    "field": field,
                    "severity": ValidationSeverity.ERROR.value,
                    "message": f"Field '{field}' has invalid email format: '{value}'",
                })
        return errors


class FreshnessValidator(BaseValidator):
    """Check that the event is not too old."""

    def __init__(self, max_age_seconds: int = 300, timestamp_field: str = "ts_ms"):
        self.max_age_seconds = max_age_seconds
        self.timestamp_field = timestamp_field

    def validate(self, payload: Dict) -> List[Dict]:
        errors = []
        ts_ms = payload.get(self.timestamp_field)
        if ts_ms is not None:
            age_seconds = time.time() - (ts_ms / 1000)
            if age_seconds > self.max_age_seconds:
                errors.append({
                    "check": "freshness",
                    "field": self.timestamp_field,
                    "severity": ValidationSeverity.WARNING.value,
                    "message": f"Event is {age_seconds:.0f}s old (max: {self.max_age_seconds}s)",
                })
        return errors


# ──────────────────────────────────────────────
# Table-specific validation rules
# ──────────────────────────────────────────────

TABLE_VALIDATORS: Dict[str, List[BaseValidator]] = {
    "cdc.public.orders": [
        NullCheckValidator(required_fields=["id", "customer_id", "amount", "status"]),
        TypeCheckValidator(field_types={"id": "int", "customer_id": "int", "amount": "string"}),
        RangeCheckValidator(field_ranges={"amount": (0.01, 1_000_000)}),
        AllowedValuesValidator(field_allowed={"status": ["pending", "completed", "cancelled", "refunded"]}),
        AllowedValuesValidator(field_allowed={"currency": ["USD", "EUR", "GBP"]}),
    ],
    "cdc.public.customers": [
        NullCheckValidator(required_fields=["id", "name", "email"]),
        TypeCheckValidator(field_types={"id": "int", "name": "string", "email": "string"}),
        EmailValidator(email_fields=["email"]),
        AllowedValuesValidator(field_allowed={"status": ["active", "inactive", "suspended"]}),
    ],
}


def validate_record(topic: str, event: Dict) -> ValidationResult:
    """
    Validate a Debezium CDC event.
    
    Debezium event structure:
    {
        "before": {...},
        "after": {...},      ← we validate this
        "source": {...},
        "op": "c|u|d|r",
        "ts_ms": 1234567890
    }
    """
    errors = []
    warnings = []

    # Extract operation type
    op = event.get("op")

    # For delete operations, skip validation (no "after" payload)
    if op == "d":
        return ValidationResult(is_valid=True, errors=[], warnings=[], table=topic)

    # Extract the "after" payload (the new row state)
    payload = event.get("after")

    if payload is None:
        return ValidationResult(
            is_valid=False,
            errors=[{
                "check": "payload_exists",
                "field": "after",
                "severity": ValidationSeverity.CRITICAL.value,
                "message": "Event has no 'after' payload",
            }],
            warnings=[],
            table=topic,
        )

    # Run freshness check on the envelope
    freshness = FreshnessValidator(max_age_seconds=300)
    freshness_issues = freshness.validate(event)
    for issue in freshness_issues:
        if issue["severity"] == ValidationSeverity.WARNING.value:
            warnings.append(issue)
        else:
            errors.append(issue)

    # Run table-specific validators
    validators = TABLE_VALIDATORS.get(topic, [])
    for validator in validators:
        issues = validator.validate(payload)
        for issue in issues:
            if issue["severity"] == ValidationSeverity.WARNING.value:
                warnings.append(issue)
            else:
                errors.append(issue)

    # Extract record key for logging
    record_key = str(payload.get("id", "unknown"))

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        record_key=record_key,
        table=topic,
    )
