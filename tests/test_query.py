
from unittest import TestCase

import pytest

from hypothesis import (
    given,
    settings,
)
from hypothesis.strategies import (
    booleans,
    just,
    lists,
    sampled_from,
    text,
)

from oedtools.query import get_columns

from .data import (
    ALL,
    DEFAULTS,
    MASTER_SCHEMA,
    NUMPY_DTYPES,
    PYTHON_DTYPES,
    REQUIRED_TYPES,
    SCHEMA_TYPES_EX_MASTER,
    SQL_DTYPES,
    SUPPORTED_SQL_DTYPES,
    VALUE_GROUPS,
)


class TestQuery(TestCase):

    @given(
        schema_types=lists(sampled_from(SCHEMA_TYPES_EX_MASTER), max_size=len(SCHEMA_TYPES_EX_MASTER), unique=True),
        headers=lists(sampled_from([header for schema_type, header in ALL]), max_size=len(ALL), unique=True),
        descriptions=lists(sampled_from(VALUE_GROUPS), max_size=len(VALUE_GROUPS), unique=True),
        required=lists(sampled_from(REQUIRED_TYPES), min_size=1, max_size=len(REQUIRED_TYPES), unique=True),
        nonnull=sampled_from([None, True, False]),
        defaults=lists(sampled_from(DEFAULTS), max_size=len(DEFAULTS), unique=True),
        python_dtypes=lists(sampled_from(PYTHON_DTYPES), max_size=len(PYTHON_DTYPES), unique=True),
        sql_dtypes=lists(sampled_from(SQL_DTYPES), max_size=len(SQL_DTYPES), unique=True),
        numpy_dtypes=lists(sampled_from(NUMPY_DTYPES), max_size=len(NUMPY_DTYPES), unique=True)
    )
    def test_get_columns(
        self,
        schema_types,
        headers,
        descriptions,
        required,
        nonnull,
        defaults,
        python_dtypes,
        sql_dtypes,
        numpy_dtypes
    ):
        #if not all(not arg for arg in [schema_types, headers, descriptions, required, nonnull, python_dtypes, sql_dtypes, numpy_dtypes]):
        #    import ipdb; ipdb.set_trace()
        results = get_columns(
            schema_types,
            headers,
            descriptions,
            required,
            nonnull,
            defaults,
            python_dtypes,
            sql_dtypes,
            numpy_dtypes
        )

        if not any([schema_types, headers, descriptions, required, nonnull, python_dtypes, sql_dtypes, numpy_dtypes]):
            self.assertEqual(results, [])
        else:
            ms = MASTER_SCHEMA
            columns = sorted(ms)

            exp_results = columns

            if schema_types:
                exp_results = set(exp_results).intersection([c for c in exp_results if c[0].lower() in schema_types])

            if headers:
                exp_results = set(exp_results).intersection([c for c in exp_results for header in headers if header.lower() in c[1].lower()])

            if descriptions:
                exp_results = set(exp_results).intersection([c for c in exp_results for desc in descriptions if desc.lower() in ms[c]['desc'].lower()])

            if required:
                exp_results = set(exp_results).intersection([c for c in exp_results if ms[c]['required'] in required])

            if nonnull is not None:
                exp_results = set(exp_results).intersection([c for c in exp_results if ms[c]['blank'] != nonnull])

            if defaults:
                exp_results = set(exp_results).intersection([c for c in exp_results if ms[c]['default'] is not None and ms[c]['default'] in defaults])

            if python_dtypes:
                exp_results = set(exp_results).intersection([c for c in exp_results if ms[c]['py_dtype'].lower() in python_dtypes])

            if sql_dtypes:
                exp_results = set(exp_results).intersection([c for c in exp_results for sql_dtype in sql_dtypes if sql_dtype.lower() in ms[c]['sql_dtype'].lower()])

            if numpy_dtypes:
                exp_results = set(exp_results).intersection([c for c in exp_results for np_dtype in numpy_dtypes if np_dtype.lower() in ms[c]['numpy_dtype'].lower()])

            exp_results = sorted([ms[c] for c in exp_results], key=lambda r: r['field_name'])

            self.assertEqual(len(results), len(exp_results))
            for r in results:
                self.assertIn(r, exp_results)
