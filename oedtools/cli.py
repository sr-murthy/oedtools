__all__ = [
    'QueryCmd',
    'OedToolsCmd',
    'SampleCmd',
    'ValidateCmd',
    'ValidateFileCmd',
    'ValidateHeadersCmd',
    'VersionCmd'
]

import io
import json
import os
import re
import sys

from argparse import RawDescriptionHelpFormatter
from ast import literal_eval
from itertools import groupby

from argparsetree import BaseCommand
from future.utils import raise_with_traceback

from .exceptions import (
    CommandError,
    ReportingError,
)
from .query import get_columns
from .report import (
    report_headers,
    report_file,
)
from .schema import (
    sample_column,
    SCHEMA_DIR,
)
from .utils import get_value


class QueryCmd(BaseCommand):
    formatter_class = RawDescriptionHelpFormatter

    def add_args(self, parser):
        """
        Command parser setup
        """
        super(self.__class__, self).add_args(parser)

        parser.add_argument(
            '-t', '--schema-types', required=False,
            help='List of file schema types; must be one of "acc", "loc", "reinsinfo", "reinsscope" - a comma-separated string enclosed in quotation marks'
        )
        parser.add_argument(
            '-m', '--column-headers', required=False,
            help='List of column headers or header substrings - a comma-separated string enclosed in quotation marks'
        )
        parser.add_argument(
            '-d', '--descriptions', required=False,
            help='List of column descriptions or description substrings - a comma-separated string enclosed in quotation marks'
        )
        parser.add_argument(
            '-r', '--required', default=None, required=False,
            help='Is the column required (R), conditionally required (CR) or optional (O)?'
        )
        parser.add_argument(
            '-n', '--nonnull', default=None, required=False, action='store_true',
            help='Is the column required not to have any null values?'
        )
        parser.add_argument(
            '-e', '--defaults', required=False,
            help='List of default values - a comma-separated string enclosed in quotation marks'
        )
        parser.add_argument(
            '-p', '--python-dtypes', required=False,
            help='List of Python data types - only "int", "float", "str" are supported; a comma-separated string enclosed in quotation marks'
        )
        parser.add_argument(
            '-s', '--sql-dtypes', required=False,
            help='List of SQL data types - a comma-separated string enclosed in quotation marks'
        )
        parser.add_argument(
            '-y', '--numpy-dtypes', required=False,
            help='List of Numpy data types - a comma-separated string enclosed in quotation marks'
        )
        parser.add_argument(
            '-a', '--headers-only', required=False, action='store_true',
            help='Only return the column headers'
        )

    def action(self, args):
        """
        Command logic
        """
        theargs = vars(args)

        def double_quote(st):
            return '"{}"'.format(st)

        schema_types = theargs['schema_types']
        if schema_types:
            schema_types = [v.strip() for v in literal_eval(double_quote(schema_types)).split(',')]

        headers = theargs['column_headers']
        if headers:
            headers = [v.strip() for v in literal_eval(double_quote(headers)).split(',')]

        descriptions = theargs['descriptions']
        if descriptions:
            descriptions = [v.strip() for v in literal_eval(double_quote(descriptions)).split(',')]

        required = theargs['required']
        if required:
            required = [v.strip() for v in literal_eval(double_quote(required)).split(',')]

        nonnull = theargs['nonnull']

        defaults = theargs['defaults']
        if defaults:
            defaults = [get_value(v.strip()) for v in literal_eval(double_quote(defaults)).split(',')]

        py_dtypes = theargs['python_dtypes']
        if py_dtypes:
            py_dtypes = [v.strip() for v in literal_eval(double_quote(py_dtypes)).split(',')]

        sql_dtypes = theargs['sql_dtypes']
        if sql_dtypes:
            sql_dtypes = [v.strip() for v in literal_eval(double_quote(sql_dtypes)).split(',')]

        np_dtypes = theargs['numpy_dtypes']
        if np_dtypes:
            np_dtypes = [v.strip() for v in literal_eval(double_quote(np_dtypes)).split(',')]

        results = get_columns(
            schema_types=schema_types,
            headers=headers,
            descriptions=descriptions,
            required=required,
            nonnull=nonnull,
            defaults=defaults,
            python_dtypes=py_dtypes,
            sql_dtypes=sql_dtypes,
            numpy_dtypes=np_dtypes
        )

        headers_only = theargs['headers_only']

        if headers_only:
            results = [
                '{} ({})'.format(column, ', '.join([g[1] for g in group]))
                for column, group in groupby(
                    [(r['field_name'], r['entity']) for r in results],
                    key=lambda t: t[0]
                )
            ]
        else:
            results = [
                {
                    k: v
                    if k not in ['column_range', 'column_validation', 'dtype_range']
                    else v if not isinstance(v, range) else str(v)
                    for k, v in r.items()
                }
                for r in results
            ]

        print(json.dumps(results, indent=4, sort_keys=True))


class SampleCmd(BaseCommand):
    formatter_class = RawDescriptionHelpFormatter

    def add_args(self, parser):
        """
        Command parser setup
        """
        super(self.__class__, self).add_args(parser)

        parser.add_argument(
            '-t', '--schema-type', required=True,
            help='List of file schema types; must be one of "acc", "loc", "reinsinfo", "reinsscope" - a comma-separated string enclosed in quotation marks'
        )
        parser.add_argument(
            '-m', '--column-header', required=True,
            help='Column header'
        )
        parser.add_argument(
            '-n', '--sample-size', required=False, type=int, default=10,
            help='Sample size'
        )

    def action(self, args):
        """
        Command logic
        """
        theargs = vars(args)

        schema_type = theargs['schema_type'].lower()

        header = theargs['column_header'].lower()

        size = theargs['sample_size']

        sample = sample_column(schema_type, header, size=size)

        print(json.dumps(sample, indent=4, sort_keys=True))


class ValidateFileCmd(BaseCommand):
    formatter_class = RawDescriptionHelpFormatter

    def add_args(self, parser):
        """
        Command parser setup
        """
        super(self.__class__, self).add_args(parser)

        parser.add_argument(
            '-f', '--input-file-path', required=True,
            help='OED input file path',
        )
        parser.add_argument(
            '-t', '--schema-type', required=True,
            help='File schema type - "loc", "acc", "reinsinfo", or "reinsscope"'
        )

    def action(self, args):
        """
        Command logic
        """
        theargs = vars(args)

        input_fp = os.path.abspath(theargs['input_file_path'])

        schema_type = theargs['schema_type'].lower()

        try:
            for line in report_file(theargs['schema_type'], theargs['input_file_path']):
                print(line)
        except ReportingError as e:
            print(e)
            sys.exit(-1)


class ValidateHeadersCmd(BaseCommand):
    formatter_class = RawDescriptionHelpFormatter

    def add_args(self, parser):
        """
        Command parser setup
        """
        super(self.__class__, self).add_args(parser)

        parser.add_argument(
            '-f', '--input-file-path', required=False,
            help='OED input file path',
        )
        parser.add_argument(
            '-e', '--column-headers', required=False,
            help='A list of column headers (a single string enclosed in double quotes, headers within string separated by commas)'
        )
        parser.add_argument(
            '-t', '--schema-type', required=True,
            help='File schema type - "loc", "acc", "reinsinfo", or "reinsscope"'
        )

    def action(self, args):
        """
        Command logic
        """
        theargs = vars(args)
        file_or_headers = None

        try:
            file_or_headers = os.path.abspath(theargs.get('input_file_path'))
        except (TypeError, ValueError):
            try:
                file_or_headers = literal_eval(theargs.get('column_headers')).strip().split(',')
            except (TypeError, ValueError):
                pass

        if not file_or_headers:
            raise CommandError(
                'Invalid arguments - please check that the input file path is '
                'a valid file path, or the headers list was provided as a '
                'string enclosed with double quotes'
            )

        schema_type = theargs['schema_type'].lower()

        try:
            for line in report_headers(schema_type, file_or_headers):
                print(line)
        except ReportingError as e:
            raise_with_traceback(CommandError(e))
            sys.exit(-1)


class VersionCmd(BaseCommand):
    formatter_class = RawDescriptionHelpFormatter

    def add_args(self, parser):
        """
        Command parser setup
        """
        super(self.__class__, self).add_args(parser)

        parser.add_argument(
            '-k', '--package', default=None, required=False, action='store_true',
            help='Get the package version?'
        )

    def action(self, args):
        """
        Command logic
        """
        theargs = vars(args)
        pkg_version = theargs.get('package')

        if pkg_version:
            init_fp = os.path.join(os.path.abspath(os.path.dirname(__file__)), '__init__.py')
            with io.open(init_fp, encoding='utf-8') as f:
                return re.search('__version__ = [\'"]([^\'"]+)[\'"]', f.read()).group(1)

        schema_ver_fp = os.path.join(SCHEMA_DIR, 'schema_version.txt')
        with io.open(schema_ver_fp, 'r', encoding='utf-8') as f:
            return f.read().strip()


class ValidateCmd(BaseCommand):
    """
    Subcommands
    ::

        * validating headers of OED input files or dict arrays (lists, tuples) of headers
        * validating OED input files or dict arrays (lists, tuples) of headers
    """
    sub_commands = {
        'headers': ValidateHeadersCmd,
        'file': ValidateFileCmd
    }

class OedToolsCmd(BaseCommand):
    """
    Root command
    """
    sub_commands = {
        'query': QueryCmd,
        'sample': SampleCmd,
        'validate': ValidateCmd,
        'version': VersionCmd
    }

    def run(self, args=None):
        """
        """
        try:
            return super(self.__class__, self).run(args=args)
        except Exception as e:
            print(e)
            sys.exit(-1)
