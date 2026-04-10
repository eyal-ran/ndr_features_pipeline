"""Task-0 canonical contract matrix and error taxonomy validators."""

from .validator import (
    ContractDriftError,
    ContractValidationError,
    ErrorTaxonomyValidationError,
    assert_contract_surface,
    assert_error_codes_declared,
    assert_no_producer_only_fields,
    load_contract_matrix,
    load_error_taxonomy,
)

__all__ = [
    "ContractDriftError",
    "ContractValidationError",
    "ErrorTaxonomyValidationError",
    "assert_contract_surface",
    "assert_error_codes_declared",
    "assert_no_producer_only_fields",
    "load_contract_matrix",
    "load_error_taxonomy",
]
