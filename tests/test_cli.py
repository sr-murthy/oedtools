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
    floats,
    integers,
    sampled_from,
)

from oedtools.__init__ import __version__ as pkg_version
from oedtools.cli import *
from oedtools.exceptions import OedError
from oedtools.schema import SCHEMA_DIR

from .data import (
    ALL,
    SCHEMA_TYPES,
    GROUPED_SCHEMA,
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
        schema_type=sampled_from(SCHEMA_TYPES),
        sample_size=integers(min_value=1, max_value=10)
    )
    def test_sample_cmd__missing_or_invalid_arguments__raises_oed_or_type_error(self, schema_type, sample_size):
        header = np.random.choice(list(GROUPED_SCHEMA[schema_type]))
        with self.assertRaises(OedError):
            SampleCmd().run(argparse.Namespace(schema_type='INVALID', column_header=header, sample_size=sample_size))
        with self.assertRaises(OedError):
            SampleCmd().run(argparse.Namespace(schema_type=schema_type, column_header='INVALID', sample_size=sample_size))
        with self.assertRaises(TypeError):
            SampleCmd().run(argparse.Namespace(schema_type=schema_type, column_header=header, sample_size=float(sample_size)))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        sample_size=integers(min_value=1, max_value=10)
    )
    def test_sample_cmd__valid_arguments__cmd_completes_successfully(self, schema_type, sample_size):
        header = np.random.choice(list(GROUPED_SCHEMA[schema_type]))
        exit_code = SampleCmd().run(argparse.Namespace(schema_type=schema_type, column_header=header, sample_size=sample_size))
        self.assertEqual(exit_code, 0)
