import re
import json
import os
from pydantic import BaseModel, Field, field_validator
from typing import Dict, Pattern

# Strict corporate email pattern: valid local part, single @, enterprise.com domain
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@enterprise\.com$")

class TicketIngestionSchema(BaseModel):
    ticket_id: str = Field(..., description="Unique alphanumeric tracking string.")
    reporter_email: str = Field(..., description="Corporate sender communication handle.")
    raw_text: str = Field(..., description="Unstructured description text payload.")

    @field_validator('reporter_email')
    @classmethod
    def validate_corporate_domain(cls, value: str) -> str:
        if not EMAIL_REGEX.match(value):
            raise ValueError("Must be a valid corporate address matching [user]@enterprise.com")
        return value

class DataSanitizationPipeline:
    def __init__(self, dlq_path: str = "dead_letter_queue.log"):
        self.dlq_path = dlq_path
        # Enhanced PII patterns for comprehensive data scrubbing
        self.pii_patterns: Dict[str, Pattern] = {
            "ssn": re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
            "credit_card": re.compile(r'\b\d{4}-\d{4}-\d{4}-\d{4}\b'),
            # Additional PII patterns
            "email": re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            "phone": re.compile(r'\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}\b'),
            "api_key": re.compile(r'\b[A-Za-z0-9_-]{32,}\b'),  # Generic API key pattern
            "ip_address": re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b'),
        }
        # Patterns that should NOT be scrubbed (whitelist for corporate domains)
        self.safe_patterns = [
            re.compile(r'@enterprise\.com$'),  # Allow corporate emails
        ]

    def _is_safe_match(self, text: str, match: re.Match) -> bool:
        """Check if a match should be whitelisted (not scrubbed)"""
        matched_text = match.group(0)
        for pattern in self.safe_patterns:
            if pattern.search(matched_text):
                return True
        return False

    def scrub_pii(self, text: str) -> str:
        """Enhanced PII scrubbing with whitelist protection"""
        sanitized = text

        # Process each PII pattern
        for label, pattern in self.pii_patterns.items():
            def replace_func(match):
                # Check if match should be whitelisted
                if self._is_safe_match(text, match):
                    return match.group(0)
                return f"[{label.upper()}_REDACTED]"

            sanitized = pattern.sub(replace_func, sanitized)

        return sanitized

    def route_to_dlq(self, error_message: str, unvalidated_payload: dict):
        with open(self.dlq_path, "a") as dlq_file:
            entry = {
                "error": error_message,
                "payload": unvalidated_payload
            }
            dlq_file.write(json.dumps(entry) + "\n")