__all__ = [
    'OedValidator'
]

import builtins
import datetime
import os

from itertools import (
    starmap,
)

import numpy as np
import pandas as pd

from future.utils import raise_with_traceback

from .exceptions import (
    get_file_error,
    OedError,
    ProcessError,
)
from .schema import (
    get_column_schema,
    get_grouped_master_schema,
    get_schema,
    get_schema_version,
    get_values_profile,
)
from .utils import (
    get_method,
    get_value,
    is_real_number,
    within_range,
)


class OedValidator(object):
    """
    The main OED input file validation class.
    """

    # Attributes which store the OED master schema + schemas for the loc. +
    # acc. + RI info. + RI scope files
    master_schema = get_schema()
    grouped_master_schema = get_grouped_master_schema()
    loc_schema = get_schema(schema_type='loc')
    acc_schema = get_schema(schema_type='acc')
    reinsinfo_schema = get_schema(schema_type='reinsinfo')
    reinsscope_schema = get_schema(schema_type='reinsscope')

    schema_version = get_schema_version()

    values_profile = get_values_profile()

    def validate_headers(self, schema_type, file_or_headers):
        """
        Validates an iterable of OED input file headers (column headers), where
        the file type is specified using one of the following strings: ``loc``
        for the loc. file, ``acc`` for the acc. file, The validation is a
        simple process of checking whether the given column headers correspond to
        columns defined in the current OED master schema and specifically the
        schema defined for the file type. Results are yielded as a dict array,
        one dict per header.

        :param schema_type: The file schema type (``loc``, ``acc``, ``reinsinfo`` or
                          ``reinsscope``).
        :type schema_type: str

        :param file_or_headers: An OED input file path or iterable of column
                                  headers from such a file
        :type file_or_headers: str, list, tuple
        """
        try:
            _schema_type = schema_type.lower()
            if _schema_type not in self.grouped_master_schema:
                raise ProcessError
        except (AttributeError, ProcessError):
            raise ProcessError(
                '"{}" is not a valid OED schema type - one of "acc", "loc", '
                '"reinsinfo" or "reinsscope" is expected'
            )

        if (
            (isinstance(file_or_headers, str) and not os.path.isfile(file_or_headers)) or
            ((isinstance(file_or_headers, list) or isinstance(file_or_headers, tuple)) and any(not isinstance(v, str) for v in file_or_headers))
            or not file_or_headers
        ):
            raise ProcessError(
                msg=(
                    '"{}" is either not a (valid) OED input CSV file path '
                    'or not a list or tuple of column headers'
                    .format(file_or_headers)
                )
            )

        is_file = isinstance(file_or_headers, str)

        try:
            headers = (
                pd.read_csv(
                    file_or_headers,
                    dtype=object,
                    float_precision='high',
                    memory_map=True
                ).columns.tolist() if is_file
                else file_or_headers
            )
        except (IOError, FileNotFoundError, TypeError, ValueError) as e:
            raise ProcessError(
                msg=(
                    'A Pandas error was encountered trying to read the file: {}. '.format(e)
                )
            )

        schema = getattr(self, '{}_schema'.format(schema_type))

        header_str = ','.join(headers)

        missing_req = sorted([
            v['field_name'] for v in schema.values()
            if v['entity'].lower() == _schema_type
            and v['required'] == 'R' and
            v['field_name'].lower() not in [h.lower() for h in 
            headers]
        ])

        for header in headers + missing_req:
            r = None
            row = 1
            column_pos = header_str.index(header) + 1 if header in headers else -1
            if header in headers:
                r = {
                    'header': header,
                    'row': row,
                    'column_pos': column_pos,
                    'exceptions': [
                        get_file_error(
                            'non oed schema and column',
                            '"{}" is an invalid OED schema type and "{}" is not a column in any OED schema'.format(schema_type, header)
                        )
                        if _schema_type not in self.grouped_master_schema
                        and not any(header.lower() in self.grouped_master_schema[stype] for stype in self.grouped_master_schema)
                        else None,
                        get_file_error(
                            'non oed schema',
                            '"{}" is an invalid OED schema type'.format(schema_type)
                        )
                        if _schema_type not in self.grouped_master_schema
                        and any(header.lower() in self.grouped_master_schema[stype] for stype in self.grouped_master_schema)
                        else None,
                        get_file_error(
                            'non oed column',
                            '"{}" is not a valid column in any OED schema'.format(header)
                        )
                        if _schema_type in self.grouped_master_schema
                        and not any(header.lower() in self.grouped_master_schema[stype] for stype in self.grouped_master_schema)
                        else None,
                        get_file_error(
                            'non oed schema column',
                            '"{}" is an invalid column in the OED "{}" schema'.format(header, schema_type)
                        )
                        if _schema_type in self.grouped_master_schema
                        and header.lower() not in self.grouped_master_schema[_schema_type]
                        else None
                    ],
                    'pass': None,
                    'required_but_missing': False
                }
                r['exceptions'] = [(1, e) for e in r['exceptions'] if e is not None]
                r['pass'] = True if not r['exceptions'] else False
            else:
                r = {
                    'header': header,
                    'row': row,
                    'column_pos': column_pos,
                    'exceptions': [
                        (1, get_file_error(
                            'missing required column',
                            '"{}" is a required column in an OED "{}" file but is missing'.format(header, schema_type)
                        ))
                    ],
                    'pass': False,
                    'required_but_missing': True
                }
            yield r

    def validate_column(self, schema_type, header, data, column_pos=None):
        """
        Validates column header and data. Results are yielded as a dict array, one
        per value

        :param schema_type: The file schema type (``loc``, ``acc``, ``reinsinfo`` or
                          ``reinsscope``).
        :type schema_type: str

        :param header: The column name
        :type header: str

        :param data: The column data iterable (list, tuple or Numpy 1D-array)
        :type data: list, tuple, np.ndarray

        :param column_pos: The index of the starting character of the column
                           name in the column header line (if known or
                           applicable)
        :type column_pos: int
        """
        _schema_type = schema_type.lower()
        _header = header.lower()

        try:
            col_schema = get_column_schema(_schema_type, _header)
        except OedError as e:
            return {
                'pass': False,
                'exceptions': [e]
            }

        exp_dtype = col_schema['py_dtype']

        if not (isinstance(data, list) or isinstance(data, tuple) or isinstance(data, np.ndarray)):
            raise ProcessError(
                'The column data/values must be passed as a list or tuple'
            )

        try:
            _exp_dtype = (
                getattr(builtins, exp_dtype) if not exp_dtype == 'datetime.datetime'
                else datetime.datetime
            )
        except AttributeError:
            raise ProcessError(
                'The expected data type string "{}" found in the "{}" column '
                'schema does not correspond to a valid Python data type'
                .format(exp_dtype, header)
            )

        if _exp_dtype not in [int, float, str]:
            raise ProcessError(
                'Currently type validation of column data is only supported '
                'for the following literal data types: "int", "float", "str"'
            )

        use_range = col_schema['column_range'] or col_schema['dtype_range']

        validation_src = col_schema['column_validation']

        validation_func = (
            get_method(validation_src.replace('func:', '')) if isinstance(validation_src, str) and validation_src.startswith('func:')
            else None
        )

        is_nonnull_col = not col_schema['blank']

        def _validate_value(row_idx, value):
            _value = get_value(value)
            if _exp_dtype == float and isinstance(_value, int):
                _value = float(_value)

            exceptions = []

            if is_nonnull_col and _value in [None, '']:
                exceptions = [
                    get_file_error(
                        'null data in non null column',
                        'Null value in "{}" - this is a non-null column'.format(header)
                    )
                ]
            elif _value not in [None, ''] and (
                (_exp_dtype is int and not isinstance(_value, int)) or
                (_exp_dtype is float and not is_real_number(_value)) or
                (_exp_dtype is str and not (isinstance(_value, str) or isinstance(_value, int)))
            ):
                exceptions += [
                    get_file_error(
                        'invalid data type',
                        'Invalid data type for value "{}" in "{}" - expected type "{}", found type "{}"'.format(_value, header, _exp_dtype, type(_value))
                    )
                ]
            elif _value not in [None, ''] and validation_func is None and use_range is not None and not within_range(use_range, _value):
                exceptions += [
                    get_file_error(
                        'data out of range',
                        'Invalid value "{}" in "{}" - check the column or data type range'.format(_value, header)
                    )
                ]
            elif _value not in [None, ''] and validation_func is not None and not validation_func(use_range, _value):
                exceptions += [
                    get_file_error(
                        'data out of range',
                        'Invalid value "{}" in "{}" - check the column or data type range'.format(_value, header)
                    )
                ]
            exceptions = [(row_idx + 2, e) for e in exceptions if e is not None]

            return {
                'header': header,
                'value': _value,
                'row': row_idx + 2,
                'column_pos': column_pos,
                'exceptions': exceptions,
                'pass': True if not exceptions else False
            }

        for _, r in zip(data, starmap(_validate_value, enumerate(data))):
            yield r

    def validate(self, schema_type, file_or_data):
        """
        Validates an OED input file, or an iterable of row dicts from an OED
        input file, against the corresponding OED schema for the given file
        type.

        :param schema_type: The file schema type (``loc``, ``acc``, ``reinsinfo`` or
                          ``reinsscope``).
        :type schema_type: str

        :param file_or_data: An OED input file path or row dict array
        :type file_or_data: str, list, tuple

        :return: A dict array of results (one per column), the overall result
                (``True`` or ``False``), and the iterable of raw headers
        :rtype: list, str, list
        """
        try:
            _schema_type = schema_type.lower()
            if _schema_type not in self.grouped_master_schema:
                raise ProcessError
        except (AttributeError, ProcessError):
            raise ProcessError(
                '"{}" is not a valid OED schema type - one of "acc", "loc", '
                '"reinsinfo" or "reinsscope" is expected'
            )

        if (
            (isinstance(file_or_data, str) and not os.path.isfile(file_or_data)) or
            ((isinstance(file_or_data, list) or isinstance(file_or_data, tuple)) and any(not isinstance(v, dict) for v in file_or_data))
        ):
            raise ProcessError(
                msg=(
                    '"{}" is either not a (valid) OED input CSV file path '
                    'or not a dict array of rows from an OED input CSV'
                    .format(file_or_data)
                )
            )

        is_file = isinstance(file_or_data, str)

        try:
            df = (
                pd.read_csv(
                    file_or_data,
                    dtype=object,
                    float_precision='high',
                    memory_map=True
                ) if is_file
                else pd.DataFrame(file_or_data, dtype=object)
            )
        except (IOError, FileNotFoundError, ValueError) as e:
            raise ProcessError(
                msg=(
                    'A Pandas error was encountered trying to read the file or row dict array: {}. '
                    'Check that the data source is valid'
                    .format(e)
                )
            )

        df = df.where(df.notnull(), None)

        raw_headers = df.columns.tolist()

        try:
            results = [
                {
                    **r,
                    **{
                        'data_results': [
                            r for r in self.validate_column(
                                schema_type, r['header'], df[r['header']].tolist(), r['column_pos']
                            )
                        ] if r['pass'] is True and not r['required_but_missing'] else []
                    }
                }
                for r in self.validate_headers(schema_type, raw_headers)
            ]
        except ProcessError as e:
            raise_with_traceback(e)

        overall_pass = True

        for col_res in results:
            row_errors = [(row['row'], e) for row in col_res['data_results'] for _, e in row['exceptions']]
            col_res['exceptions'] += row_errors
            col_res['exceptions'] = list(set(col_res['exceptions']))
            if col_res['exceptions']:
                col_res['pass'] = False
                if overall_pass is True:
                    overall_pass = False

        return results, overall_pass, raw_headers
