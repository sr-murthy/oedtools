import builtins
import importlib
import string

from random import shuffle
from tempfile import NamedTemporaryFile
from unittest import TestCase

import numpy as np
import pandas as pd
import pytest

from hypothesis import (
    given,
    settings,
)
from hypothesis.strategies import (
    binary,
    booleans,
    complex_numbers,
    fixed_dictionaries,
    floats,
    integers,
    just,
    lists,
    none,
    one_of,
    sampled_from,
    text,
    tuples,
)

from oedtools.exceptions import (
    ReportingError,
)
from oedtools.report import (
    report_file,
    report_headers,
)

from .data import (
    ALL,
    ACC,
    ACC_NONNULL,
    ACC_OPTIONAL,
    ACC_REQUIRED,
    ACC_OPTIONAL,
    get_method,
    get_value,
    GROUPED_SCHEMA,
    is_real_number,
    LOC,
    LOC_NONNULL,
    LOC_OPTIONAL,
    LOC_REQUIRED,
    MASTER_SCHEMA,
    NONNULL,
    NUMERIC,
    OPTIONAL,
    REINSINFO,
    REINSINFO_NONNULL,
    REINSINFO_OPTIONAL,
    REINSINFO_REQUIRED,
    REINSSCOPE,
    REINSSCOPE_NONNULL,
    REINSSCOPE_OPTIONAL,
    REINSSCOPE_REQUIRED,
    REQUIRED,
    REQUIRED_NONNULL,
    sample_column,
    SCHEMA_TYPES,
    STRING_WITH_FINITE_RANGE,
    SUPPORTED_SQL_DTYPES,
    VALUE_GROUPS,
)


class TestReport(TestCase):

    @given(
        schema_type=text(),
        file_or_headers=one_of(text(), lists(text(), max_size=0))
    )
    def test_report_headers__invalid_schema_type_and_file_or_headers__oed_reporting_error_raised(self, schema_type, file_or_headers):
        with self.assertRaises(ReportingError):
            list(report_headers(schema_type, file_or_headers))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        fp=text(min_size=1, alphabet=string.ascii_letters + string.digits + '-.')
    )
    def test_report_headers__invalid_file__oed_reporting_error_raised(self, schema_type, fp):
        with self.assertRaises(ReportingError):
            list(report_headers(schema_type, fp))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        headers=lists(text(), max_size=0)
    )
    def test_report_headers__invalid_or_no_headers__oed_reporting_error_raised(self, schema_type, headers):
        with self.assertRaises(ReportingError):
            list(report_headers(schema_type, headers))
