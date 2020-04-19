import os
import re
import string

from itertools import (
    chain,
    product,
)
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
    integers,
    lists,
    one_of,
    sampled_from,
    text,
)

from oedtools.exceptions import (
    ReportingError,
)
from oedtools.report import (
    report_file,
    report_headers,
)
from oedtools.validate import (
    OedValidator,
)

from .data import (
    GROUPED_SCHEMA,
    sample_column,
    SCHEMA_TYPES,
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

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=0, max_size=10, unique=True)
    )
    def test_report_headers__random_valid_schema_type__headers_as_list__report_correctly_generated(self, schema_type, non_oed):
        num_headers = np.random.choice(range(1, len(GROUPED_SCHEMA[schema_type])))
        oed = np.random.choice(list(GROUPED_SCHEMA[schema_type]), size=num_headers, replace=False).tolist()
        non_oed = ['non oed ' + s for s in non_oed]

        required_but_missing = [h for h, v in GROUPED_SCHEMA[schema_type].items() if v['required'] == 'R' and h not in oed]

        headers = oed + non_oed
        shuffle(headers)

        report = report_headers(schema_type, headers)

        if not (required_but_missing or non_oed):
            self.assertEqual(list(report), [])
        else:
            for line in report:
                self.assertIsNotNone(re.match(r'^:1:(-)?\d+:.*$', line))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=0, max_size=10, unique=True)
    )
    def test_report_headers__random_valid_schema_type__headers_as_file__report_correctly_generated(self, schema_type, non_oed):
        num_headers = np.random.choice(range(1, len(GROUPED_SCHEMA[schema_type])))
        oed = np.random.choice(list(GROUPED_SCHEMA[schema_type]), size=num_headers, replace=False).tolist()
        non_oed = ['non oed ' + s for s in non_oed]

        required_but_missing = [h for h, v in GROUPED_SCHEMA[schema_type].items() if v['required'] == 'R' and h not in oed]

        headers = oed + non_oed
        shuffle(headers)
        header_str = ','.join(headers)

        with NamedTemporaryFile('w') as file:
            file.write(header_str + '\n')
            file.flush()

            report = report_headers(schema_type, file.name)

            if not (required_but_missing or non_oed):
                self.assertEqual(list(report), [])
            else:
                for line in report:
                    self.assertIsNotNone(re.match(r'^{}:1.*$'.format(file.name), line))

    @given(
        schema_type=text(),
        file_or_headers=one_of(text(), lists(text(), max_size=0))
    )
    def test_report_file__invalid_schema_type_and_file_or_headers__oed_reporting_error_raised(self, schema_type, file_or_headers):
        with self.assertRaises(ReportingError):
            list(report_file(schema_type, file_or_headers))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        fp=text(min_size=1, alphabet=string.ascii_letters + string.digits + '-.')
    )
    def test_report_file__invalid_file__oed_reporting_error_raised(self, schema_type, fp):
        with self.assertRaises(ReportingError):
            list(report_file(schema_type, fp))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES)
    )
    def test_report_file__invalid_or_no_headers__oed_reporting_error_raised(self, schema_type):
        with NamedTemporaryFile('w') as file:
            with self.assertRaises(ReportingError):
                list(report_headers(schema_type, file.name))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=0, max_size=3, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    @settings(max_examples=10, deadline=None)
    def test_report_file__random_valid_schema_type__data_as_dict_array__report_correctly_generated(self, schema_type, non_oed, num_rows):
        num_headers = np.random.choice(range(1, len(GROUPED_SCHEMA[schema_type])))
        oed = np.random.choice(list(GROUPED_SCHEMA[schema_type]), size=num_headers, replace=False).tolist()
        if non_oed:
            non_oed = ['non oed ' + s for s in non_oed]

        required_but_missing = [h for h, v in GROUPED_SCHEMA[schema_type].items() if v['required'] == 'R' and h not in oed]

        headers = oed + non_oed
        shuffle(headers)

        data = pd.DataFrame(data={
            header: sample_column('loc', 'flexiloczzz', str_width=5, size=num_rows)
            if header in non_oed
            else sample_column(schema_type, header, size=num_rows)
            for header in headers
        }).to_dict('records')

        report = report_file(schema_type, data)

        if not (required_but_missing or non_oed):
            self.assertEqual(list(report), [])
        else:
            for line in report:
                self.assertIsNotNone(re.match(r'^:1:(-)?\d+:.*$', line))

    @given(
        schema_type=sampled_from(SCHEMA_TYPES),
        non_oed=lists((text(alphabet=string.ascii_letters, min_size=1)), min_size=0, max_size=3, unique=True),
        num_rows=integers(min_value=1, max_value=10)
    )
    @settings(max_examples=10, deadline=None)
    def test_report_file__random_valid_schema_type__data_as_file__report_correctly_generated(self, schema_type, non_oed, num_rows):
        num_headers = np.random.choice(range(1, len(GROUPED_SCHEMA[schema_type])))
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

            report = report_file(schema_type, file.name)

            if not (required_but_missing or non_oed):
                self.assertEqual(list(report), [])
            else:
                for line in report:
                    self.assertIsNotNone(re.match(r'^{}:1.*$'.format(file.name), line))
