import argparse
import io
import os
import string

from unittest import TestCase

import numpy as np

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
)

from oedtools.__init__ import __version__ as pkg_version
from oedtools.cli import *
from oedtools.exceptions import OedError
from oedtools.schema import SCHEMA_DIR

from .data import (
    ALL,
    DEFAULTS,
    DESCRIPTION_WORDS,
    GROUPED_SCHEMA,
    NUMPY_DTYPES,
    PYTHON_DTYPES,
    REQUIRED_TYPES,
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
        sample_size=integers(min_value=1, max_value=10)
    )
    def test_sample_cmd__invalid_schema_type_or_column_header__raises_oed_error(self, schema_type, sample_size):
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
        sample_size=integers(min_value=1, max_value=10)
    )
    def test_sample_cmd__valid_arguments__cmd_completes_successfully(self, schema_type, sample_size):
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