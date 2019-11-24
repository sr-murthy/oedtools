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
    DataOutOfRangeError,
    InvalidDataTypeError,
    MissingRequiredColumnError,
    NonOedColumnError,
    NonOedSchemaError,
    NonOedSchemaColumnError,
    NonOedSchemaAndColumnError,
    NullDataInNonNullColumnError,
    ProcessError,
)
from oedtools.validate import OedValidator

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


class TestValidate(TestCase):

    def setUp(self):
        self.validator = OedValidator()

    @given(
        schema_type=text(),
        file_or_headers=one_of(text(), lists(text(), max_size=0))
    )
    def test_validate_headers__invalid_schema_type_and_file_or_headers__oed_validation_process_error_raised(self, schema_type, file_or_headers):
        with self.assertRaises(ProcessError):
            list(self.validator.validate_headers(schema_type, file_or_headers))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        fp=text(min_size=1, alphabet=string.ascii_letters + string.digits + '-.')
    )
    def test_validate_headers__invalid_file__oed_validation_process_error_raised(self, schema_type, fp):
        with self.assertRaises(ProcessError):
            list(self.validator.validate_headers(schema_type, fp))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        headers=lists(text(), max_size=0)
    )
    def test_validate_headers__invalid_or_no_headers__oed_validation_process_error_raised(self, schema_type, headers):
        with self.assertRaises(ProcessError):
            list(self.validator.validate_headers(schema_type, headers))

    @given(
        required=just(LOC_REQUIRED),
        optional=lists(sampled_from(LOC_OPTIONAL), min_size=1, max_size=len(LOC_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__loc__all_required_headers_present__some_non_oed_headers__only_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('loc', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header in oed:
                self.assertEqual(exceptions, [])
            else:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)

    @given(
        required=just(LOC_REQUIRED),
        optional=lists(sampled_from(LOC_OPTIONAL), min_size=1, max_size=len(LOC_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__loc__as_file__all_required_headers_present__some_non_oed_headers__only_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as loc_file:
            loc_file.write(header_str + '\n')
            loc_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('loc', loc_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header in oed:
                    self.assertEqual(exceptions, [])
                else:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 2)
                    self.assertIsInstance(exceptions[0], NonOedColumnError)
                    self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)

    @given(
        required=lists(sampled_from(LOC_REQUIRED), min_size=1, max_size=len(LOC_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(LOC_OPTIONAL), min_size=1, max_size=len(LOC_OPTIONAL), unique=True)
    )
    def test_validate_headers__loc__some_required_headers_missing__no_non_oed_headers__only_required_but_missing_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        required_missing = set(LOC_REQUIRED).difference(required)
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers + required_missing, self.validator.validate_headers('loc', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header not in required_missing else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header not in required_missing:
                self.assertEqual(exceptions, [])
            else:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(LOC_REQUIRED), min_size=1, max_size=len(LOC_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(LOC_OPTIONAL), min_size=1, max_size=len(LOC_OPTIONAL), unique=True)
    )
    def test_validate_headers__loc__some_required_headers_missing__no_non_oed_headers__only_required_but_missing_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        required_missing = sorted(set(LOC_REQUIRED).difference(required))
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as loc_file:
            loc_file.write(header_str + '\n')
            loc_file.flush()

            for header, header_res in zip(headers + required_missing, self.validator.validate_headers('loc', loc_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header not in required_missing else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header not in required_missing:
                    self.assertEqual(exceptions, [])
                else:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 1)
                    self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(LOC_REQUIRED), min_size=1, max_size=len(LOC_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(LOC_OPTIONAL), min_size=1, max_size=len(LOC_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__loc__some_required_headers_missing__some_non_oed_headers__required_but_missing_column_errors_and_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        required_missing = sorted(set(LOC_REQUIRED).difference(required))
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('loc', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header in oed:
                self.assertEqual(exceptions, [])
            elif header in non_oed:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in required_missing:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(LOC_REQUIRED), min_size=1, max_size=len(LOC_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(LOC_OPTIONAL), min_size=1, max_size=len(LOC_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__loc__some_required_headers_missing__some_non_oed_headers__required_but_missing_column_errors_and_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        required_missing = sorted(set(LOC_REQUIRED).difference(required))
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as loc_file:
            loc_file.write(header_str + '\n')
            loc_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('loc', loc_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header in oed:
                    self.assertEqual(exceptions, [])
                elif header in non_oed:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 2)
                    self.assertIsInstance(exceptions[0], NonOedColumnError)
                    self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
                elif header in required_missing:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 1)
                    self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=just(LOC_REQUIRED),
        optional=lists(sampled_from(LOC_OPTIONAL), min_size=1, max_size=len(LOC_OPTIONAL), unique=True)
    )
    def test_validate_headers__loc__all_required_headers_present__no_non_oed_headers__no_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('loc', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            self.assertEqual(exceptions, [])

    @given(
        required=just(LOC_REQUIRED),
        optional=lists(sampled_from(LOC_OPTIONAL), min_size=1, max_size=len(LOC_OPTIONAL), unique=True)
    )
    def test_validate_headers__loc__as_file__all_required_headers_present__no_non_oed_headers__no_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as loc_file:
            loc_file.write(header_str + '\n')
            loc_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('loc', loc_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                self.assertEqual(exceptions, [])

    @given(
        required=just(ACC_REQUIRED),
        optional=lists(sampled_from(ACC_OPTIONAL), min_size=1, max_size=len(ACC_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__acc__all_required_headers_present__some_non_oed_headers__only_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('acc', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header in oed:
                self.assertEqual(exceptions, [])
            else:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)

    @given(
        required=just(ACC_REQUIRED),
        optional=lists(sampled_from(ACC_OPTIONAL), min_size=1, max_size=len(ACC_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__acc___as_file__all_required_headers_present__some_non_oed_headers__only_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as acc_file:
            acc_file.write(header_str + '\n')
            acc_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('acc', acc_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header in oed:
                    self.assertEqual(exceptions, [])
                else:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertIsInstance(exceptions[0], NonOedColumnError)
                    self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)

    @given(
        required=lists(sampled_from(ACC_REQUIRED), min_size=1, max_size=len(ACC_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(ACC_OPTIONAL), min_size=1, max_size=len(ACC_OPTIONAL), unique=True)
    )
    def test_validate_headers__acc__some_required_headers_missing__no_non_oed_headers__only_required_but_missing_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        required_missing = sorted(set(ACC_REQUIRED).difference(required))
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers + required_missing, self.validator.validate_headers('acc', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header not in required_missing else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header not in required_missing:
                self.assertEqual(exceptions, [])
            else:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(ACC_REQUIRED), min_size=1, max_size=len(ACC_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(ACC_OPTIONAL), min_size=1, max_size=len(ACC_OPTIONAL), unique=True)
    )
    def test_validate_headers__acc__as_file__some_required_headers_missing__no_non_oed_headers__only_required_but_missing_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        required_missing = sorted(set(ACC_REQUIRED).difference(required))
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as acc_file:
            acc_file.write(header_str + '\n')
            acc_file.flush()

            for header, header_res in zip(headers + required_missing, self.validator.validate_headers('acc', acc_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header not in required_missing else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header not in required_missing:
                    self.assertEqual(exceptions, [])
                else:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 1)
                    self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(ACC_REQUIRED), min_size=1, max_size=len(ACC_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(ACC_OPTIONAL), min_size=1, max_size=len(ACC_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__acc__some_required_headers_missing__some_non_oed_headers__required_but_missing_column_errors_and_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        required_missing = sorted(set(ACC_REQUIRED).difference(required))
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('acc', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header in oed:
                self.assertEqual(exceptions, [])
            elif header in non_oed:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in required_missing:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(ACC_REQUIRED), min_size=1, max_size=len(ACC_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(ACC_OPTIONAL), min_size=1, max_size=len(ACC_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__acc__as_file__some_required_headers_missing__some_non_oed_headers__required_but_missing_column_errors_and_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        required_missing = sorted(set(ACC_REQUIRED).difference(required))
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as acc_file:
            acc_file.write(header_str + '\n')
            acc_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('acc', acc_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header in oed:
                    self.assertEqual(exceptions, [])
                elif header in non_oed:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 2)
                    self.assertIsInstance(exceptions[0], NonOedColumnError)
                    self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
                elif header in required_missing:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 1)
                    self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=just(ACC_REQUIRED),
        optional=lists(sampled_from(ACC_OPTIONAL), min_size=1, max_size=len(ACC_OPTIONAL), unique=True)
    )
    def test_validate_headers__acc__all_required_headers_present__no_non_oed_headers__no_column_errors_generated(
        self,
        required,
        optional
    ):
        #import ipdb; ipdb.set_trace()
        oed = required + optional
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('acc', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            self.assertEqual(exceptions, [])

    @given(
        required=just(ACC_REQUIRED),
        optional=lists(sampled_from(ACC_OPTIONAL), min_size=1, max_size=len(ACC_OPTIONAL), unique=True)
    )
    def test_validate_headers__acc__as_file__all_required_headers_present__no_non_oed_headers__no_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as acc_file:
            acc_file.write(header_str + '\n')
            acc_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('acc', acc_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                self.assertEqual(exceptions, [])

    @given(
        required=just(REINSINFO_REQUIRED),
        optional=lists(sampled_from(REINSINFO_OPTIONAL), min_size=1, max_size=len(REINSINFO_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__reinsinfo__all_required_headers_present__some_non_oed_headers__only_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('reinsinfo', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header in oed:
                self.assertEqual(exceptions, [])
            else:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)

    @given(
        required=just(REINSINFO_REQUIRED),
        optional=lists(sampled_from(REINSINFO_OPTIONAL), min_size=1, max_size=len(REINSINFO_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__reinsinfo__as_file__all_required_headers_present__some_non_oed_headers__only_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as reinsinfo_file:
            reinsinfo_file.write(header_str + '\n')
            reinsinfo_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('reinsinfo', reinsinfo_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header in oed:
                    self.assertEqual(exceptions, [])
                else:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertIsInstance(exceptions[0], NonOedColumnError)
                    self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)

    @given(
        required=lists(sampled_from(REINSINFO_REQUIRED), min_size=1, max_size=len(REINSINFO_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(REINSINFO_OPTIONAL), min_size=1, max_size=len(REINSINFO_OPTIONAL), unique=True)
    )
    def test_validate_headers__reinsinfo__some_required_headers_missing__no_non_oed_headers__only_required_but_missing_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        required_missing = sorted(set(REINSINFO_REQUIRED).difference(required))
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers + required_missing, self.validator.validate_headers('reinsinfo', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header not in required_missing else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header not in required_missing:
                self.assertEqual(exceptions, [])
            else:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(REINSINFO_REQUIRED), min_size=1, max_size=len(REINSINFO_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(REINSINFO_OPTIONAL), min_size=1, max_size=len(REINSINFO_OPTIONAL), unique=True)
    )
    def test_validate_headers__reinsinfo__as_file__some_required_headers_missing__no_non_oed_headers__only_required_but_missing_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        required_missing = sorted(set(REINSINFO_REQUIRED).difference(required))
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as reinsinfo_file:
            reinsinfo_file.write(header_str + '\n')
            reinsinfo_file.flush()

            for header, header_res in zip(headers + required_missing, self.validator.validate_headers('reinsinfo', reinsinfo_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header not in required_missing else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header not in required_missing:
                    self.assertEqual(exceptions, [])
                else:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 1)
                    self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(REINSINFO_REQUIRED), min_size=1, max_size=len(REINSINFO_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(REINSINFO_OPTIONAL), min_size=1, max_size=len(REINSINFO_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__reinsinfo__some_required_headers_missing__some_non_oed_headers__required_but_missing_column_errors_and_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        required_missing = sorted(set(REINSINFO_REQUIRED).difference(required))
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('reinsinfo', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header in oed:
                self.assertEqual(exceptions, [])
            elif header in non_oed:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in required_missing:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(REINSINFO_REQUIRED), min_size=1, max_size=len(REINSINFO_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(REINSINFO_OPTIONAL), min_size=1, max_size=len(REINSINFO_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__reinsinfo__some_required_headers_missing__some_non_oed_headers__required_but_missing_column_errors_and_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        required_missing = sorted(set(REINSINFO_REQUIRED).difference(required))
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as reinsinfo_file:
            reinsinfo_file.write(header_str + '\n')
            reinsinfo_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('reinsinfo', reinsinfo_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header in oed:
                    self.assertEqual(exceptions, [])
                elif header in non_oed:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 2)
                    self.assertIsInstance(exceptions[0], NonOedColumnError)
                    self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
                elif header in required_missing:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 1)
                    self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=just(REINSINFO_REQUIRED),
        optional=lists(sampled_from(REINSINFO_OPTIONAL), min_size=1, max_size=len(REINSINFO_OPTIONAL), unique=True)
    )
    def test_validate_headers__reinsinfo__all_required_headers_present__no_non_oed_headers__no_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('reinsinfo', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            self.assertEqual(exceptions, [])

    @given(
        required=just(REINSINFO_REQUIRED),
        optional=lists(sampled_from(REINSINFO_OPTIONAL), min_size=1, max_size=len(REINSINFO_OPTIONAL), unique=True)
    )
    def test_validate_headers__reinsinfo__as_file__all_required_headers_present__no_non_oed_headers__no_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as reinsinfo_file:
            reinsinfo_file.write(header_str + '\n')
            reinsinfo_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('reinsinfo', reinsinfo_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                self.assertEqual(exceptions, [])

    @given(
        required=just(REINSSCOPE_REQUIRED),
        optional=lists(sampled_from(REINSSCOPE_OPTIONAL), min_size=1, max_size=len(REINSSCOPE_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__reinsscope__all_required_headers_present__some_non_oed_headers__only_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('reinsscope', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header in oed:
                self.assertEqual(exceptions, [])
            else:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)

    @given(
        required=just(REINSSCOPE_REQUIRED),
        optional=lists(sampled_from(REINSSCOPE_OPTIONAL), min_size=1, max_size=len(REINSSCOPE_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__reinsscope__as_file__all_required_headers_present__some_non_oed_headers__only_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as reinsscope_file:
            reinsscope_file.write(header_str + '\n')
            reinsscope_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('reinsscope', reinsscope_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header in oed:
                    self.assertEqual(exceptions, [])
                else:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertIsInstance(exceptions[0], NonOedColumnError)
                    self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)

    @given(
        required=lists(sampled_from(REINSSCOPE_REQUIRED), min_size=1, max_size=len(REINSSCOPE_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(REINSSCOPE_OPTIONAL), min_size=1, max_size=len(REINSSCOPE_OPTIONAL), unique=True)
    )
    def test_validate_headers__reinsscope__some_required_headers_missing__no_non_oed_headers__only_required_but_missing_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        required_missing = sorted(set(REINSSCOPE_REQUIRED).difference(required))
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers + required_missing, self.validator.validate_headers('reinsscope', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header not in required_missing else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header not in required_missing:
                self.assertEqual(exceptions, [])
            else:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(REINSSCOPE_REQUIRED), min_size=1, max_size=len(REINSSCOPE_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(REINSSCOPE_OPTIONAL), min_size=1, max_size=len(REINSSCOPE_OPTIONAL), unique=True)
    )
    def test_validate_headers__reinsscope__as_file__some_required_headers_missing__no_non_oed_headers__only_required_but_missing_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        required_missing = sorted(set(REINSSCOPE_REQUIRED).difference(required))
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as reinsscope_file:
            reinsscope_file.write(header_str + '\n')
            reinsscope_file.flush()

            for header, header_res in zip(headers + required_missing, self.validator.validate_headers('reinsscope', reinsscope_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header not in required_missing else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header not in required_missing:
                    self.assertEqual(exceptions, [])
                else:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 1)
                    self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(REINSSCOPE_REQUIRED), min_size=1, max_size=len(REINSSCOPE_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(REINSSCOPE_OPTIONAL), min_size=1, max_size=len(REINSSCOPE_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__reinsscope__some_required_headers_missing__some_non_oed_headers__required_but_missing_column_errors_and_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        required_missing = sorted(set(REINSSCOPE_REQUIRED).difference(required))
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('reinsscope', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if header in oed:
                self.assertEqual(exceptions, [])
            elif header in non_oed:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in required_missing:
                exceptions = sorted(exceptions, key=lambda e: e.code)
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=lists(sampled_from(REINSSCOPE_REQUIRED), min_size=1, max_size=len(REINSSCOPE_REQUIRED) - 1, unique=True),
        optional=lists(sampled_from(REINSSCOPE_OPTIONAL), min_size=1, max_size=len(REINSSCOPE_OPTIONAL), unique=True),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=2, unique=True)
    )
    def test_validate_headers__reinsscope__as_file__some_required_headers_missing__some_non_oed_headers__required_but_missing_column_errors_and_non_oed_column_and_non_oed_schema_column_errors_generated(
        self,
        required,
        optional,
        non_oed
    ):
        oed = required + optional
        non_oed = ['non oed ' + col for col in non_oed]
        required_missing = sorted(set(REINSSCOPE_REQUIRED).difference(required))
        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as reinsscope_file:
            reinsscope_file.write(header_str + '\n')
            reinsscope_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('reinsscope', reinsscope_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass']) if header in oed else self.assertFalse(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                if header in oed:
                    self.assertEqual(exceptions, [])
                elif header in non_oed:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 2)
                    self.assertIsInstance(exceptions[0], NonOedColumnError)
                    self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
                elif header in required_missing:
                    exceptions = sorted(exceptions, key=lambda e: e.code)
                    self.assertEqual(len(exceptions), 1)
                    self.assertIsInstance(exceptions[0], MissingRequiredColumnError)

    @given(
        required=just(REINSSCOPE_REQUIRED),
        optional=lists(sampled_from(REINSSCOPE_OPTIONAL), min_size=1, max_size=len(REINSSCOPE_OPTIONAL), unique=True)
    )
    def test_validate_headers__reinsscope__all_required_headers_present__no_non_oed_headers__no_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        for header, header_res in zip(headers, self.validator.validate_headers('reinsscope', headers)):
            self.assertEqual(header_res['header'].lower(), header.lower())
            self.assertTrue(header_res['pass'])
            self.assertEqual(header_res['row'], 1)
            self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
            exceptions = [t[1] for t in header_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            self.assertEqual(exceptions, [])

    @given(
        required=just(REINSSCOPE_REQUIRED),
        optional=lists(sampled_from(REINSSCOPE_OPTIONAL), min_size=1, max_size=len(REINSSCOPE_OPTIONAL), unique=True)
    )
    def test_validate_headers__reinsscope__as_file__all_required_headers_present__no_non_oed_headers__no_column_errors_generated(
        self,
        required,
        optional
    ):
        oed = required + optional
        headers = oed
        shuffle(headers)
        header_str = ','.join(headers)
        with NamedTemporaryFile('w') as reinsscope_file:
            reinsscope_file.write(header_str + '\n')
            reinsscope_file.flush()

            for header, header_res in zip(headers, self.validator.validate_headers('reinsscope', reinsscope_file.name)):
                self.assertEqual(header_res['header'].lower(), header.lower())
                self.assertTrue(header_res['pass'])
                self.assertEqual(header_res['row'], 1)
                self.assertEqual(header_res['column_pos'], header_str.index(header) + 1 if header in headers else -1)
                exceptions = [t[1] for t in header_res['exceptions']]
                self.assertIsInstance(exceptions, list)
                self.assertEqual(exceptions, [])

    @given(
        schema_key=sampled_from(ALL),
        num_values=integers(min_value=10, max_value=100)
    )
    def test_validate_column__random_schema_and_column__some_null_values__no_invalid_datatype_values_or_ex_range_values___only_null_value_errors_generated(
        self, schema_key, num_values
    ):
        schema_type, header = schema_key
        data = [get_value(v) for v in sample_column(schema_type, header, size=num_values)]
        col_schema = MASTER_SCHEMA[(schema_type, header)]
        is_nonnull_col = not col_schema['blank']

        for i in np.random.choice(range(len(data)), size=int(len(data) / 10), replace=False):
            data[i] = None

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)
            (
                self.assertTrue(value_res['pass']) if not is_nonnull_col or value is not None
                else self.assertFalse(value_res['pass'])
            )

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], NullDataInNonNullColumnError)

    @given(
        schema_key=sampled_from(ALL),
        num_values=integers(min_value=10, max_value=100),
        flt=floats(allow_nan=False, allow_infinity=False),
        intg=integers(),
        st=text(min_size=1, alphabet=(string.ascii_letters + string.digits + string.punctuation))
    )
    def test_validate_column__random_schema_and_column__no_null_values_or_no_ex_range_values__some_invalid_datatype_values__only_datatype_errors_generated(
        self, schema_key, num_values, flt, intg, st
    ):
        schema_type, header = schema_key
        data = sample_column(schema_type, header, size=num_values)
        col_schema = GROUPED_SCHEMA[schema_type][header]
        exp_py_dtype = getattr(builtins, col_schema['py_dtype'])
        use_range = col_schema['column_range']

        for i in np.random.choice(range(len(data)), int(len(data) / 10), replace=False):
            data[i] = flt if exp_py_dtype in [int, str] else (st + 'string')

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)
            if (
                (exp_py_dtype in [int, str] and isinstance(value_res['value'], float)) or
                (exp_py_dtype is float and (isinstance(value_res['value'], str)))
            ):
                self.assertFalse(value_res['pass'])
            else:
                self.assertTrue(value_res['pass'])

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], InvalidDataTypeError)

    @given(
        schema_key=sampled_from(NUMERIC),
        num_values=integers(min_value=10, max_value=100)
    )
    def test_validate_column__random_schema_and_numeric_column__no_null_values_or_invalid_datatype_values__some_ex_range_values__only_range_errors_generated(
        self, schema_key, num_values
    ):
        schema_type, header = schema_key
        data = sample_column(schema_type, header, size=num_values)
        col_schema = GROUPED_SCHEMA[schema_type][header]
        exp_py_dtype, use_range = getattr(builtins, col_schema['py_dtype']), col_schema['column_range'] or col_schema['dtype_range']
        range_lb, range_ub = (use_range.start, use_range.stop) if isinstance(use_range, range) else (min(use_range), max(use_range))
        validation_src = col_schema['column_validation']
        validation_func = (
            get_method(validation_src.replace('func:', '')) if isinstance(validation_src, str) and validation_src.startswith('func:')
            else None
        )

        for i in np.random.choice(range(len(data)), int(len(data) / 10), replace=False):
            data[i] = range_lb - 1 if i % 2 else range_ub + 1

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)

            if exp_py_dtype in [int, float] and validation_func is not None:
                self.assertFalse(value_res['pass']) if not validation_func(use_range, value) else self.assertTrue(value_res['pass'])
            elif exp_py_dtype is int and (isinstance(use_range, range) or isinstance(use_range, list) or isinstance(use_range, tuple)):
                self.assertTrue(value_res['pass']) if value in use_range else self.assertFalse(value_res['pass'])
            elif exp_py_dtype is float and use_range is not None:
                self.assertTrue(value_res['pass']) if (value >= min(use_range) and value <= max(use_range)) else self.assertFalse(value_res['pass'])

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], DataOutOfRangeError)

    @given(
        schema_key=sampled_from(STRING_WITH_FINITE_RANGE),
        num_values=integers(min_value=10, max_value=100)
    )
    def test_validate_column__random_schema_and_string_column_with_finite_range__no_null_values_or_invalid_datatype_values__some_ex_range_values__only_range_errors_generated(
        self, schema_key, num_values
    ):
        schema_type, header = schema_key
        data = [get_value(v) for v in sample_column(schema_type, header, size=num_values)]
        col_schema = GROUPED_SCHEMA[schema_type][header]
        use_range = col_schema['column_range']
        validation_src = col_schema['column_validation']
        validation_func = (
            get_method(validation_src.replace('func:', '')) if isinstance(validation_src, str) and validation_src.startswith('func:')
            else None
        )

        for i in np.random.choice(range(len(data)), int(len(data) / 10), replace=False):
            data[i] = 'out of range'

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)
            if validation_func is not None:
                self.assertTrue(value_res['pass']) if validation_func(use_range, value) else self.assertFalse(value_res['pass'])
            else:
                self.assertTrue(value_res['pass']) if value in use_range else self.assertFalse(value_res['pass'])

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], DataOutOfRangeError)

    @given(
        schema_key=sampled_from(ALL),
        num_values=integers(min_value=10, max_value=100),
        flt=floats(allow_nan=False, allow_infinity=False),
        intg=integers(),
        st=text(min_size=1, alphabet=(string.ascii_letters + string.digits + string.punctuation))
    )
    def test_validate_column__random_schema_and_column__some_null_values_and_invalid_datatype_values__no_ex_range_values___only_null_value_and_datatype_errors_generated(
        self, schema_key, num_values, flt, intg, st
    ):
        schema_type, header = schema_key
        data = [get_value(v) for v in sample_column(schema_type, header, size=num_values)]
        col_schema = MASTER_SCHEMA[(schema_type, header)]
        exp_py_dtype = getattr(builtins, col_schema['py_dtype'])
        is_nonnull_col = not col_schema['blank']

        for i in np.random.choice(range(len(data)), size=int(len(data) / 10), replace=False):
            data[i] = (
                None if i % 2
                else (flt if exp_py_dtype in [int, str] else st + 'string')
            )

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)
            if value is None:
                self.assertFalse(value_res['pass']) if is_nonnull_col else self.assertTrue(value_res['pass'])
            elif not isinstance(value, exp_py_dtype):
                self.assertFalse(value_res['pass']) if (
                    (exp_py_dtype in [int, str] and isinstance(value_res['value'], float)) or
                    (exp_py_dtype is float and (isinstance(value_res['value'], str)))
                ) else self.assertTrue(value_res['pass'])

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], NullDataInNonNullColumnError if value is None else InvalidDataTypeError)

    @given(
        schema_key=sampled_from(NUMERIC),
        num_values=integers(min_value=10, max_value=100)
    )
    def test_validate_column__random_schema_and_numeric_column__some_null_values_and_ex_range_values__no_invalid_datatype_values__only_null_value_and_range_errors_generated(
        self, schema_key, num_values
    ):
        schema_type, header = schema_key
        data = sample_column(schema_type, header, size=num_values)
        col_schema = MASTER_SCHEMA[(schema_type, header)]
        exp_py_dtype, use_range = getattr(builtins, col_schema['py_dtype']), col_schema['column_range'] or col_schema['dtype_range']
        is_nonnull_col = not col_schema['blank']
        range_lb, range_ub = (
            (use_range.start, use_range.stop) if isinstance(use_range, range)
            else (min(use_range), max(use_range))
        )
        validation_src = col_schema['column_validation']
        validation_func = (
            get_method(validation_src.replace('func:', '')) if isinstance(validation_src, str) and validation_src.startswith('func:')
            else None
        )

        for i in np.random.choice(range(len(data)), int(len(data) / 10), replace=False):
            data[i] = (range_lb - 1 if i % 2 else range_ub + 1) if i % 3 else None

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)
            if value is None:
                self.assertFalse(value_res['pass']) if is_nonnull_col else self.assertTrue(value_res['pass'])
            elif validation_func is not None:
                self.assertFalse(value_res['pass']) if not validation_func(use_range, value) else self.assertTrue(value_res['pass'])
            elif isinstance(value, int):
                self.assertFalse(value_res['pass']) if value not in use_range else self.assertTrue(value_res['pass'])
            else:
                self.assertFalse(value_res['pass']) if value < range_lb or value > range_ub else self.assertTrue(value_res['pass'])

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], NullDataInNonNullColumnError if value is None else DataOutOfRangeError)

    @given(
        schema_key=sampled_from(STRING_WITH_FINITE_RANGE),
        num_values=integers(min_value=10, max_value=100)
    )
    def test_validate_column__random_schema_and_string_column_with_finite_range__some_null_values_and_ex_range_values__no_invalid_datatype_values__only_null_value_and_range_errors_generated(
        self, schema_key, num_values
    ):
        schema_type, header = schema_key
        data = [get_value(v) for v in sample_column(schema_type, header, size=num_values)]
        col_schema = MASTER_SCHEMA[(schema_type, header)]
        py_dtype = col_schema['py_dtype']
        use_range = col_schema['column_range']
        validation_src = col_schema['column_validation']
        validation_func = (
            get_method(validation_src.replace('func:', '')) if isinstance(validation_src, str) and validation_src.startswith('func:')
            else None
        )
        is_nonnull_col = not col_schema['blank']

        for i in np.random.choice(range(len(data)), int(len(data) / 10), replace=False):
            data[i] = None if i % 2 else 'out of range'

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)
            if value is None:
                self.assertFalse(value_res['pass']) if is_nonnull_col else self.assertTrue(value_res['pass'])
            elif validation_func is not None:
                self.assertFalse(value_res['pass']) if not validation_func(use_range, value) or py_dtype == 'str' and isinstance(value, float) else self.assertTrue(value_res['pass'])
            elif validation_func is None:
                self.assertFalse(value_res['pass']) if value not in use_range or py_dtype == 'str' and isinstance(value, float) else self.assertTrue(value_res['pass'])

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], NullDataInNonNullColumnError if value is None else DataOutOfRangeError)

    @given(
        schema_key=sampled_from(NUMERIC),
        num_values=integers(min_value=10, max_value=100)
    )
    def test_validate_column__random_schema_and_numeric_column__some_ex_range_and_invalid_datatype_values__no_null_values__only_range_and_datatype_errors_generated(
        self, schema_key, num_values
    ):
        schema_type, header = schema_key
        data = sample_column(schema_type, header, size=num_values)
        col_schema = MASTER_SCHEMA[(schema_type, header)]
        exp_py_dtype, use_range = getattr(builtins, col_schema['py_dtype']), col_schema['column_range'] or col_schema['dtype_range']
        validation_src = col_schema['column_validation']
        validation_func = (
            get_method(validation_src.replace('func:', '')) if isinstance(validation_src, str) and validation_src.startswith('func:')
            else None
        )
        st = lambda flt: 'string'
        switch_type = {int: float, float: st}
        range_lb, range_ub = (
            (use_range.start, use_range.stop) if isinstance(use_range, range)
            else (min(use_range), max(use_range))
        )

        for i in np.random.choice(range(len(data)), int(len(data) / 10), replace=False):
            data[i] = switch_type[int](range_lb - 1) if exp_py_dtype is int else switch_type[float](range_ub + 1)

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)
            if validation_func is not None:
                self.assertFalse(value_res['pass']) if not validation_func(use_range, value) else self.assertTrue(value_res['pass'])
            elif exp_py_dtype is int:
                self.assertFalse(value_res['pass']) if not isinstance(value, int) or value not in use_range else self.assertTrue(value_res['pass'])
            else:
                self.assertFalse(value_res['pass']) if not isinstance(value, float) or (value < range_lb or value > range_ub) else self.assertTrue(value_res['pass'])

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], InvalidDataTypeError if not isinstance(value, exp_py_dtype) else DataOutOfRangeError)

    @given(
        schema_key=sampled_from(STRING_WITH_FINITE_RANGE),
        num_values=integers(min_value=10, max_value=100)
    )
    def test_validate_column__random_schema_and_string_column_with_finite_range__some_ex_range_and_invalid_datatype_values__no_null_values__only_range_and_datatype_errors_generated(
        self, schema_key, num_values
    ):
        schema_type, header = schema_key
        data = [get_value(v) for v in sample_column(schema_type, header, size=num_values)]
        col_schema = MASTER_SCHEMA[(schema_type, header)]
        py_dtype = col_schema['py_dtype']
        use_range = col_schema['column_range']
        validation_src = col_schema['column_validation']
        validation_func = (
            get_method(validation_src.replace('func:', '')) if isinstance(validation_src, str) and validation_src.startswith('func:')
            else None
        )

        for i in np.random.choice(range(len(data)), int(len(data) / 10), replace=False):
            data[i] = 'out of range' if i % 2 else float(i)

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)
            if validation_func is not None:
                self.assertFalse(value_res['pass']) if not validation_func(use_range, value) or py_dtype == 'str' and isinstance(value, float) else self.assertTrue(value_res['pass'])
            elif validation_func is None:
                self.assertFalse(value_res['pass']) if value not in use_range or py_dtype == 'str' and isinstance(value, float) else self.assertTrue(value_res['pass'])

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], InvalidDataTypeError if not isinstance(value, str) else DataOutOfRangeError)

    @given(
        schema_key=sampled_from(NUMERIC),
        num_values=integers(min_value=10, max_value=100)
    )
    def test_validate_column__random_schema_and_numeric_column__some_nulls_and_ex_range_and_invalid_datatype_values__null_value_and_range_and_datatype_errors_generated(
        self, schema_key, num_values
    ):
        schema_type, header = schema_key
        data = sample_column(schema_type, header, size=num_values)
        col_schema = MASTER_SCHEMA[(schema_type, header)]
        exp_py_dtype, use_range = getattr(builtins, col_schema['py_dtype']), col_schema['column_range'] or col_schema['dtype_range']
        validation_src = col_schema['column_validation']
        validation_func = (
            get_method(validation_src.replace('func:', '')) if isinstance(validation_src, str) and validation_src.startswith('func:')
            else None
        )
        is_nonnull_col = not col_schema['blank']
        st = lambda flt: 'string'
        switch_type = {int: float, float: st}

        range_lb, range_ub = (
            (use_range.start, use_range.stop) if isinstance(use_range, range)
            else (min(use_range), max(use_range))
        )

        for i in np.random.choice(range(len(data)), int(len(data) / 10), replace=False):
            data[i] = (
                (switch_type[int](range_lb - 1) if exp_py_dtype is int else switch_type[float](range_ub + 1)) if i % 3
                else None
            )

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)
            if value is None:
                self.assertFalse(value_res['pass']) if is_nonnull_col else self.assertTrue(value_res['pass'])
            elif validation_func is not None:
                self.assertFalse(value_res['pass']) if not validation_func(use_range, value) else self.assertTrue(value_res['pass'])
            elif exp_py_dtype is int:
                self.assertFalse(value_res['pass']) if not isinstance(value, int) or value not in use_range else self.assertTrue(value_res['pass'])
            elif exp_py_dtype is float:
                self.assertFalse(value_res['pass']) if not isinstance(value, float) or (value < range_lb or value > range_ub) else self.assertTrue(value_res['pass'])

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                if value is None:
                    self.assertIsInstance(exceptions[0], NullDataInNonNullColumnError)
                else:
                    self.assertIsInstance(exceptions[0], InvalidDataTypeError if not isinstance(value, exp_py_dtype) else DataOutOfRangeError)

    @given(
        schema_key=sampled_from(STRING_WITH_FINITE_RANGE),
        num_values=integers(min_value=10, max_value=100)
    )
    def test_validate_column__random_schema_and_string_column_with_finite_range__some_nulls_and_ex_range_and_invalid_datatype_values__null_value_and_range_and_datatype_errors_generated(
        self, schema_key, num_values
    ):
        schema_type, header = schema_key
        data = [get_value(v) for v in sample_column(schema_type, header, size=num_values)]
        col_schema = MASTER_SCHEMA[(schema_type, header)]
        py_dtype = col_schema['py_dtype']
        use_range = col_schema['column_range']
        validation_src = col_schema['column_validation']
        validation_func = (
            get_method(validation_src.replace('func:', '')) if isinstance(validation_src, str) and validation_src.startswith('func:')
            else None
        )
        is_nonnull_col = not col_schema['blank']

        for i in np.random.choice(range(len(data)), int(len(data) / 10), replace=False):
            data[i] = ('out of range' if i % 2 else float(i)) if i % 3 else None

        for value, value_res in zip(data, self.validator.validate_column(schema_type, header, data)):
            self.assertIsInstance(value_res, dict)
            self.assertEqual(value_res['header'], header)
            self.assertEqual(value_res['value'], value)
            if value is None:
                self.assertFalse(value_res['pass']) if is_nonnull_col else self.assertTrue(value_res['pass'])
            elif validation_func is not None:
                self.assertFalse(value_res['pass']) if not validation_func(use_range, value) or py_dtype == 'str' and isinstance(value, float) else self.assertTrue(value_res['pass'])
            elif validation_func is None:
                self.assertFalse(value_res['pass']) if value not in use_range  or py_dtype == 'str' and isinstance(value, float) else self.assertTrue(value_res['pass'])

            exceptions = [t[1] for t in value_res['exceptions']]
            self.assertIsInstance(exceptions, list)
            if not value_res['pass']:
                self.assertEqual(len(exceptions), 1)
                if value is None:
                    self.assertIsInstance(exceptions[0], NullDataInNonNullColumnError)
                else:
                    self.assertIsInstance(exceptions[0], InvalidDataTypeError if not isinstance(value, str) else DataOutOfRangeError)

    @settings(max_examples=10)
    @given(
        schema_type=text(),
        file_or_data=one_of(text(), binary(), complex_numbers(), booleans(), fixed_dictionaries({'a': integers()}))
    )
    def test_validate__invalid_schema_type_and_file_or_data__oed_validation_process_error_raised(self, schema_type, file_or_data):
        with self.assertRaises(ProcessError):
            self.validator.validate(schema_type, file_or_data)

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        file=text(min_size=1, alphabet=string.ascii_letters + string.digits + '-.')
    )
    def test_validate__invalid_file__oed_validation_process_error_raised(self, schema_type, file):
        with self.assertRaises(ProcessError):
            self.validator.validate(schema_type, file)

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        data=fixed_dictionaries({'a': integers()})
    )
    def test_validate__invalid_data__oed_validation_process_error_raised(self, schema_type, data):
        with self.assertRaises(ProcessError):
            self.validator.validate(schema_type, data)

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        data=lists(lists(one_of(text(), integers(), floats())), min_size=1)
    )
    def test_validate__invalid_data__no_headers__oed_validation_process_error_raised(self, schema_type, data):
        with self.assertRaises(ProcessError):
            self.validator.validate(schema_type, data)

    @settings(deadline=None)
    @given(
        required=lists(sampled_from(LOC_REQUIRED), min_size=1, max_size=len(LOC_REQUIRED), unique=True),
        optional=lists(sampled_from(LOC_OPTIONAL), max_size=len(LOC_OPTIONAL), unique=True),
        non_oed=lists(text(alphabet=(string.ascii_letters + string.digits), min_size=1), max_size=5, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    def test_validate__loc__no_bad_data_in_oed_columns__some_non_oed_columns_present__missing_required_column_and_non_oed_column_errors_generated(
        self, required, optional, non_oed, num_rows
    ):
        missing = sorted(set(LOC_REQUIRED).difference(required))
        non_oed = sorted(set(['non oed ' + col.lower() for col in non_oed]))
        headers = sorted(required + optional + non_oed)
        
        df = pd.DataFrame(data={
            header: sample_column('loc', 'flexiloczzz', str_width=5, size=num_rows)
            if header in non_oed
            else sample_column('loc', header, size=num_rows)
            for header in headers
        })

        data = df.to_dict(orient='records')

        header_str = ','.join(df.columns.tolist())

        results, overall, raw_headers = self.validator.validate('loc', data)

        self.assertFalse(overall) if missing or non_oed else self.assertTrue(overall)

        self.assertEqual(set(raw_headers), set(headers))

        self.assertIsInstance(results, list)
        self.assertEqual(len(results), len(headers + missing))

        for r, header in zip(results, headers + missing):
            self.assertEqual(r['header'].lower(), header)
            self.assertFalse(r['pass']) if header in missing + non_oed else self.assertTrue(r['pass'])
            self.assertTrue(r['required_but_missing']) if header in missing else self.assertFalse(r['required_but_missing'])
            self.assertEqual(r['column_pos'], header_str.index(header) + 1) if header not in missing else self.assertEqual(r['column_pos'], -1)
            exceptions = sorted([e[1] for e in r['exceptions']], key=lambda e: e.code)
            if header in non_oed:
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in missing:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)
            elif header in required + optional:
                self.assertEqual(exceptions, [])

    @settings(deadline=None)
    @given(
        required=lists(sampled_from(LOC_REQUIRED), min_size=1, max_size=len(LOC_REQUIRED), unique=True),
        optional=lists(sampled_from(LOC_OPTIONAL), max_size=len(LOC_OPTIONAL), unique=True),
        non_oed=lists(text(alphabet=(string.ascii_letters + string.digits), min_size=1), max_size=5, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    def test_validate__loc__as_file__no_bad_data_in_oed_columns__some_non_oed_columns_present__missing_required_column_and_non_oed_column_errors_generated(
        self, required, optional, non_oed, num_rows
    ):
        if 'areacode' not in optional:
            optional += ['areacode']
        missing = sorted(set(LOC_REQUIRED).difference(required))
        non_oed = sorted(set(['non oed ' + col.lower() for col in non_oed]))
        headers = sorted(required + optional + non_oed)
        
        with NamedTemporaryFile('w') as loc_file:
            pd.DataFrame(data={
                header: sample_column('loc', 'flexiloczzz', str_width=5, size=num_rows)
                if header in non_oed
                else sample_column('loc', header, size=num_rows)
                for header in headers
            }).to_csv(path_or_buf=loc_file.name, index=False, encoding='utf-8')

            header_str = ','.join(pd.read_csv(loc_file.name).columns.tolist())

            results, overall, raw_headers = self.validator.validate('loc', loc_file.name)

        self.assertFalse(overall) if missing or non_oed else self.assertTrue(overall)

        self.assertEqual(set(raw_headers), set(headers))

        self.assertIsInstance(results, list)
        self.assertEqual(len(results), len(headers + missing))

        for r, header in zip(results, headers + missing):
            self.assertEqual(r['header'].lower(), header)
            self.assertFalse(r['pass']) if header in missing + non_oed else self.assertTrue(r['pass'])
            self.assertTrue(r['required_but_missing']) if header in missing else self.assertFalse(r['required_but_missing'])
            self.assertEqual(r['column_pos'], header_str.index(header) + 1) if header not in missing else self.assertEqual(r['column_pos'], -1)
            exceptions = sorted([e[1] for e in r['exceptions']], key=lambda e: e.code)
            if header in non_oed:
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in missing:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)
            elif header in required + optional:
                self.assertEqual(exceptions, [])

    @settings(deadline=None)
    @given(
        required=lists(sampled_from(ACC_REQUIRED), min_size=1, max_size=len(ACC_REQUIRED), unique=True),
        optional=lists(sampled_from(ACC_OPTIONAL), max_size=len(ACC_OPTIONAL), unique=True),
        non_oed=lists(text(alphabet=(string.ascii_letters + string.digits), min_size=1), max_size=5, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    def test_validate__acc__no_bad_data_in_oed_columns__some_non_oed_columns_present__missing_required_column_and_non_oed_column_errors_generated(
        self, required, optional, non_oed, num_rows
    ):
        missing = sorted(set(ACC_REQUIRED).difference(required))
        non_oed = sorted(set(['non oed ' + col.lower() for col in non_oed]))
        headers = sorted(required + optional + non_oed)
        
        df = pd.DataFrame(data={
            header: sample_column('acc', 'flexiacczzz', str_width=5, size=num_rows)
            if header in non_oed
            else sample_column('acc', header, size=num_rows)
            for header in headers
        })

        data = df.to_dict(orient='records')

        header_str = ','.join(df.columns.tolist())

        results, overall, raw_headers = self.validator.validate('acc', data)

        self.assertFalse(overall) if missing or non_oed else self.assertTrue(overall)

        self.assertEqual(set(raw_headers), set(headers))

        self.assertIsInstance(results, list)
        self.assertEqual(len(results), len(headers + missing))

        for r, header in zip(results, headers + missing):
            self.assertEqual(r['header'].lower(), header)
            self.assertFalse(r['pass']) if header in missing + non_oed else self.assertTrue(r['pass'])
            self.assertTrue(r['required_but_missing']) if header in missing else self.assertFalse(r['required_but_missing'])
            self.assertEqual(r['column_pos'], header_str.index(header) + 1) if header not in missing else self.assertEqual(r['column_pos'], -1)
            exceptions = sorted([e[1] for e in r['exceptions']], key=lambda e: e.code)
            if header in non_oed:
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in missing:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)
            elif header in required + optional:
                self.assertEqual(exceptions, [])

    @settings(deadline=None)
    @given(
        required=lists(sampled_from(ACC_REQUIRED), min_size=1, max_size=len(ACC_REQUIRED), unique=True),
        optional=lists(sampled_from(ACC_OPTIONAL), max_size=len(ACC_OPTIONAL), unique=True),
        non_oed=lists(text(alphabet=(string.ascii_letters + string.digits), min_size=1), max_size=5, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    def test_validate__acc__as_file__no_bad_data_in_oed_columns__some_non_oed_columns_present__missing_required_column_and_non_oed_column_errors_generated(
        self, required, optional, non_oed, num_rows
    ):
        missing = sorted(set(ACC_REQUIRED).difference(required))
        non_oed = sorted(set(['non oed ' + col.lower() for col in non_oed]))
        headers = sorted(required + optional + non_oed)
        
        with NamedTemporaryFile('w') as acc_file:
            pd.DataFrame(data={
                header: sample_column('acc', 'flexiacczzz', str_width=5, size=num_rows)
                if header in non_oed
                else sample_column('acc', header, size=num_rows)
                for header in headers
            }).to_csv(path_or_buf=acc_file.name, index=False, encoding='utf-8')

            header_str = ','.join(pd.read_csv(acc_file.name).columns.tolist())

            results, overall, raw_headers = self.validator.validate('acc', acc_file.name)

        self.assertFalse(overall) if missing or non_oed else self.assertTrue(overall)

        self.assertEqual(set(raw_headers), set(headers))

        self.assertIsInstance(results, list)
        self.assertEqual(len(results), len(headers + missing))

        for r, header in zip(results, headers + missing):
            self.assertEqual(r['header'].lower(), header)
            self.assertFalse(r['pass']) if header in missing + non_oed else self.assertTrue(r['pass'])
            self.assertTrue(r['required_but_missing']) if header in missing else self.assertFalse(r['required_but_missing'])
            self.assertEqual(r['column_pos'], header_str.index(header) + 1) if header not in missing else self.assertEqual(r['column_pos'], -1)
            exceptions = sorted([e[1] for e in r['exceptions']], key=lambda e: e.code)
            if header in non_oed:
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in missing:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)
            elif header in required + optional:
                self.assertEqual(exceptions, [])

    @settings(deadline=None)
    @given(
        required=lists(sampled_from(REINSINFO_REQUIRED), min_size=1, max_size=len(REINSINFO_REQUIRED), unique=True),
        optional=lists(sampled_from(REINSINFO_OPTIONAL), max_size=len(REINSINFO_OPTIONAL), unique=True),
        non_oed=lists(text(alphabet=(string.ascii_letters + string.digits), min_size=1), max_size=5, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    def test_validate__reinsinfo__no_bad_data_in_oed_columns__some_non_oed_columns_present__missing_required_column_and_non_oed_column_errors_generated(
        self, required, optional, non_oed, num_rows
    ):
        missing = sorted(set(REINSINFO_REQUIRED).difference(required))
        non_oed = sorted(set(['non oed ' + col.lower() for col in non_oed]))
        headers = sorted(required + optional + non_oed)
        
        df = pd.DataFrame(data={
            header: sample_column('reinsinfo', 'reinsname', str_width=5, size=num_rows)
            if header in non_oed
            else sample_column('reinsinfo', header, size=num_rows)
            for header in headers
        })

        data = df.to_dict(orient='records')

        header_str = ','.join(df.columns.tolist())

        results, overall, raw_headers = self.validator.validate('reinsinfo', data)

        self.assertFalse(overall) if missing or non_oed else self.assertTrue(overall)

        self.assertEqual(set(raw_headers), set(headers))

        self.assertIsInstance(results, list)
        self.assertEqual(len(results), len(headers + missing))

        for r, header in zip(results, headers + missing):
            self.assertEqual(r['header'].lower(), header)
            self.assertFalse(r['pass']) if header in missing + non_oed else self.assertTrue(r['pass'])
            self.assertTrue(r['required_but_missing']) if header in missing else self.assertFalse(r['required_but_missing'])
            self.assertEqual(r['column_pos'], header_str.index(header) + 1) if header not in missing else self.assertEqual(r['column_pos'], -1)
            exceptions = sorted([e[1] for e in r['exceptions']], key=lambda e: e.code)
            if header in non_oed:
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in missing:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)
            elif header in required + optional:
                self.assertEqual(exceptions, [])

    @settings(deadline=None)
    @given(
        required=lists(sampled_from(REINSINFO_REQUIRED), min_size=1, max_size=len(REINSINFO_REQUIRED), unique=True),
        optional=lists(sampled_from(REINSINFO_OPTIONAL), max_size=len(REINSINFO_OPTIONAL), unique=True),
        non_oed=lists(text(alphabet=(string.ascii_letters + string.digits), min_size=1), max_size=5, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    def test_validate__reinsinfo__as_file__no_bad_data_in_oed_columns__some_non_oed_columns_present__missing_required_column_and_non_oed_column_errors_generated(
        self, required, optional, non_oed, num_rows
    ):
        missing = sorted(set(REINSINFO_REQUIRED).difference(required))
        non_oed = sorted(set(['non oed ' + col.lower() for col in non_oed]))
        headers = sorted(required + optional + non_oed)
        
        with NamedTemporaryFile('w') as reinsinfo_file:
            pd.DataFrame(data={
                header: sample_column('reinsinfo', 'reinsname', str_width=5, size=num_rows)
                if header in non_oed
                else sample_column('reinsinfo', header, size=num_rows)
                for header in headers
            }).to_csv(path_or_buf=reinsinfo_file.name, index=False, encoding='utf-8')

            header_str = ','.join(pd.read_csv(reinsinfo_file.name).columns.tolist())

            results, overall, raw_headers = self.validator.validate('reinsinfo', reinsinfo_file.name)

        self.assertFalse(overall) if missing or non_oed else self.assertTrue(overall)

        self.assertEqual(set(raw_headers), set(headers))

        self.assertIsInstance(results, list)
        self.assertEqual(len(results), len(headers + missing))

        for r, header in zip(results, headers + missing):
            self.assertEqual(r['header'].lower(), header)
            self.assertFalse(r['pass']) if header in missing + non_oed else self.assertTrue(r['pass'])
            self.assertTrue(r['required_but_missing']) if header in missing else self.assertFalse(r['required_but_missing'])
            self.assertEqual(r['column_pos'], header_str.index(header) + 1) if header not in missing else self.assertEqual(r['column_pos'], -1)
            exceptions = sorted([e[1] for e in r['exceptions']], key=lambda e: e.code)
            if header in non_oed:
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in missing:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)
            elif header in required + optional:
                self.assertEqual(exceptions, [])

    @settings(deadline=None)
    @given(
        required=lists(sampled_from(REINSSCOPE_REQUIRED), min_size=1, max_size=len(REINSSCOPE_REQUIRED), unique=True),
        optional=lists(sampled_from(REINSSCOPE_OPTIONAL), max_size=len(REINSSCOPE_OPTIONAL), unique=True),
        non_oed=lists(text(alphabet=(string.ascii_letters + string.digits), min_size=1), max_size=5, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    def test_validate__reinsscope__no_bad_data_in_oed_columns__some_non_oed_columns_present__missing_required_column_and_non_oed_column_errors_generated(
        self, required, optional, non_oed, num_rows
    ):
        missing = sorted(set(REINSSCOPE_REQUIRED).difference(required))
        non_oed = sorted(set(['non oed ' + col.lower() for col in non_oed]))
        headers = sorted(required + optional + non_oed)
        
        df = pd.DataFrame(data={
            header: sample_column('reinsscope', 'reinstag', str_width=5, size=num_rows)
            if header in non_oed
            else sample_column('reinsscope', header, size=num_rows)
            for header in headers
        })

        data = df.to_dict(orient='records')

        header_str = ','.join(df.columns.tolist())

        results, overall, raw_headers = self.validator.validate('reinsscope', data)

        self.assertFalse(overall) if missing or non_oed else self.assertTrue(overall)

        self.assertEqual(set(raw_headers), set(headers))

        self.assertIsInstance(results, list)
        self.assertEqual(len(results), len(headers + missing))

        for r, header in zip(results, headers + missing):
            self.assertEqual(r['header'].lower(), header)
            self.assertFalse(r['pass']) if header in missing + non_oed else self.assertTrue(r['pass'])
            self.assertTrue(r['required_but_missing']) if header in missing else self.assertFalse(r['required_but_missing'])
            self.assertEqual(r['column_pos'], header_str.index(header) + 1) if header not in missing else self.assertEqual(r['column_pos'], -1)
            exceptions = sorted([e[1] for e in r['exceptions']], key=lambda e: e.code)
            if header in non_oed:
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in missing:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)
            elif header in required + optional:
                self.assertEqual(exceptions, [])

    @settings(deadline=None)
    @given(
        required=lists(sampled_from(REINSSCOPE_REQUIRED), min_size=1, max_size=len(REINSSCOPE_REQUIRED), unique=True),
        optional=lists(sampled_from(REINSSCOPE_OPTIONAL), max_size=len(REINSSCOPE_OPTIONAL), unique=True),
        non_oed=lists(text(alphabet=(string.ascii_letters + string.digits), min_size=1), max_size=5, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    def test_validate__reinsscope__as_file__no_bad_data_in_oed_columns__some_non_oed_columns_present__missing_required_column_and_non_oed_column_errors_generated(
        self, required, optional, non_oed, num_rows
    ):
        missing = sorted(set(REINSSCOPE_REQUIRED).difference(required))
        non_oed = sorted(set(['non oed ' + col.lower() for col in non_oed]))
        headers = sorted(required + optional + non_oed)
        
        with NamedTemporaryFile('w') as reinsscope_file:
            pd.DataFrame(data={
                header: sample_column('reinsscope', 'reinstag', str_width=5, size=num_rows)
                if header in non_oed
                else sample_column('reinsscope', header, size=num_rows)
                for header in headers
            }).to_csv(path_or_buf=reinsscope_file.name, index=False, encoding='utf-8')

            header_str = ','.join(pd.read_csv(reinsscope_file.name).columns.tolist())

            results, overall, raw_headers = self.validator.validate('reinsscope', reinsscope_file.name)

        self.assertFalse(overall) if missing or non_oed else self.assertTrue(overall)

        self.assertEqual(set(raw_headers), set(headers))

        self.assertIsInstance(results, list)
        self.assertEqual(len(results), len(headers + missing))

        for r, header in zip(results, headers + missing):
            self.assertEqual(r['header'].lower(), header)
            self.assertFalse(r['pass']) if header in missing + non_oed else self.assertTrue(r['pass'])
            self.assertTrue(r['required_but_missing']) if header in missing else self.assertFalse(r['required_but_missing'])
            self.assertEqual(r['column_pos'], header_str.index(header) + 1) if header not in missing else self.assertEqual(r['column_pos'], -1)
            exceptions = sorted([e[1] for e in r['exceptions']], key=lambda e: e.code)
            if header in non_oed:
                self.assertEqual(len(exceptions), 2)
                self.assertIsInstance(exceptions[0], NonOedColumnError)
                self.assertIsInstance(exceptions[1], NonOedSchemaColumnError)
            elif header in missing:
                self.assertEqual(len(exceptions), 1)
                self.assertIsInstance(exceptions[0], MissingRequiredColumnError)
            elif header in required + optional:
                self.assertEqual(exceptions, [])
