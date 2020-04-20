import argparse
import io
import os
import string

from random import shuffle
from tempfile import NamedTemporaryFile
from unittest import TestCase

import numpy as np
import pandas as pd

from hypothesis import (
    given,
    settings,
)
from hypothesis.strategies import (
    booleans,
    data,
    floats,
    integers,
    lists,
    sampled_from,
    text,
)

from oedtools.__init__ import __version__ as pkg_version
from oedtools.cli import *
from oedtools.exceptions import (
    CommandError,
    OedError,
)
from oedtools.schema import SCHEMA_DIR

from .data import (
    ALL,
    DEFAULTS,
    DESCRIPTION_WORDS,
    GROUPED_SCHEMA,
    NUMPY_DTYPES,
    PYTHON_DTYPES,
    REQUIRED_TYPES,
    sample_column,
    SCHEMA_TYPES_EX_MASTER,
    SQL_DTYPES,
)


class TestCli(TestCase):

    def test_version_cmd__get_oed_version(self):
        schema_ver_fp = os.path.join(SCHEMA_DIR, 'schema_version.txt')
        with io.open(schema_ver_fp, 'r', encoding='utf-8') as f:
            expected_oed_version = f.read().strip()

        cmd_oed_version = VersionCmd().run(argparse.Namespace())

        self.assertEqual(expected_oed_version, cmd_oed_version)

    def test_version_cmd__get_package_version(self):
        expected_pkg_version = pkg_version

        cmd_pkg_version = VersionCmd().run(argparse.Namespace(package=True))

        self.assertEqual(expected_pkg_version, cmd_pkg_version)

    @given(
        schema_type=sampled_from(SCHEMA_TYPES_EX_MASTER),
        header=sampled_from(ALL),
        sample_size=integers(min_value=1, max_value=10)
    )
    def test_sample_cmd__invalid_schema_type_or_column_header__raises_oed_error(self, schema_type, header, sample_size):
        if header not in GROUPED_SCHEMA[schema_type]:
            header = np.random.choice(list(GROUPED_SCHEMA[schema_type]))
        args = {
            'schema_type': np.random.choice([schema_type, 'INVALID']),
            'column_header': np.random.choice([header, 'INVALID']),
            'sample_size': sample_size
        }
        if set(args.values()) == {'INVALID'}:
            args['schema_type'] = 'INVALID'
        with self.assertRaises(OedError):
            SampleCmd().run(argparse.Namespace(**args))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES_EX_MASTER),
        header=sampled_from(ALL),
        sample_size=integers(min_value=1, max_value=10)
    )
    def test_sample_cmd__valid_arguments__cmd_completes_successfully(self, schema_type, header, sample_size):
        if header not in GROUPED_SCHEMA[schema_type]:
            header = np.random.choice(list(GROUPED_SCHEMA[schema_type]))
        exit_code = SampleCmd().run(argparse.Namespace(schema_type=schema_type, column_header=header, sample_size=sample_size))
        self.assertEqual(exit_code, 0)

    @given(
        schema_types=lists(sampled_from(SCHEMA_TYPES_EX_MASTER), min_size=1, max_size=len(SCHEMA_TYPES_EX_MASTER), unique=True),
        headers=lists(sampled_from([header for schema_type, header in ALL]), max_size=10, unique=True),
        descriptions=lists(sampled_from(DESCRIPTION_WORDS), max_size=2, unique=True),
        required=lists(sampled_from(REQUIRED_TYPES), min_size=1, max_size=len(REQUIRED_TYPES), unique=True),
        nonnull=sampled_from([None, True, False]),
        defaults=lists(sampled_from(DEFAULTS), max_size=2, unique=True),
        python_dtypes=lists(sampled_from(PYTHON_DTYPES), max_size=2, unique=True),
        sql_dtypes=lists(sampled_from(SQL_DTYPES), max_size=2, unique=True),
        numpy_dtypes=lists(sampled_from(NUMPY_DTYPES), max_size=2, unique=True),
        headers_only=booleans()
    )
    def test_query_cmd__valid_arguments__cmd_completes_successfully(
        self, schema_types, headers, descriptions, required, nonnull,
        defaults, python_dtypes, sql_dtypes, numpy_dtypes, headers_only
    ):
        exit_code = QueryCmd().run(argparse.Namespace(
            schema_types=schema_types,
            column_headers=headers,
            descriptions=descriptions,
            required=required,
            nonnull=nonnull,
            defaults=defaults,
            python_dtypes=python_dtypes,
            sql_dtypes=sql_dtypes,
            numpy_dtypes=numpy_dtypes,
            headers_only=headers_only
        ))
        self.assertEqual(exit_code, 0)

    @given(
        schema_type=sampled_from(SCHEMA_TYPES_EX_MASTER),
        headers=lists(sampled_from([header for schema_type, header in ALL])),
        as_file=booleans()
    )
    def test_validate_headers_cmd__valid_schema_type_and_column_headers__cmd_completes_successfully(self, schema_type, headers, as_file):
        if not (headers or (headers and set(headers).issubset(list(GROUPED_SCHEMA[schema_type])))):
            headers = np.random.choice(list(GROUPED_SCHEMA[schema_type]), 10, replace=False).tolist()
        headers_str = ','.join(headers)
        if not as_file:
            exit_code = ValidateHeadersCmd().run(argparse.Namespace(schema_type=schema_type, column_headers=headers_str))
            self.assertEqual(exit_code, 0)
        else:
            with NamedTemporaryFile('w') as file:
                file.write(headers_str)
                file.flush()
                exit_code = ValidateHeadersCmd().run(argparse.Namespace(schema_type=schema_type, input_file_path=file.name))
                self.assertEqual(exit_code, 0)

    @given(
        schema_type=sampled_from(SCHEMA_TYPES_EX_MASTER),
        num_headers=integers(min_value=1),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=0, max_size=3, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    @settings(max_examples=10, deadline=None)
    def test_validate_file_cmd__valid_schema_type_and_file__cmd_completes_successfully(self, schema_type, num_headers, non_oed, num_rows):
        num_headers = min(num_headers, len(GROUPED_SCHEMA[schema_type]))
        oed = np.random.choice(list(GROUPED_SCHEMA[schema_type]), size=num_headers, replace=False).tolist()
        if non_oed:
            non_oed = ['non oed ' + s for s in non_oed]

        required_but_missing = [h for h, v in GROUPED_SCHEMA[schema_type].items() if v['required'] == 'R' and h not in oed]

        headers = oed + non_oed
        shuffle(headers)

        with NamedTemporaryFile('w') as file:
            pd.DataFrame(data={
                header: sample_column('loc', 'flexiloczzz', str_width=5, size=num_rows)
                if header in non_oed
                else sample_column(schema_type, header, size=num_rows)
                for header in headers
            }).to_csv(path_or_buf=file.name, index=False, encoding='utf-8')
            exit_code = ValidateFileCmd().run(argparse.Namespace(schema_type=schema_type, input_file_path=file.name))
            self.assertEqual(exit_code, 0)
