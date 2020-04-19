import argparse
import io
import os

from unittest import TestCase

from oedtools.__init__ import __version__ as pkg_version
from oedtools.cli import *
from oedtools.schema import SCHEMA_DIR


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
