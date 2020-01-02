__all__ = [
    'report_file',
    'report_headers'
]

from itertools import (
    chain,
    product,
)

from future.utils import raise_with_traceback

from .exceptions import (
    ProcessError,
    ReportingError,
)
from .validate import OedValidator


def report_headers(schema_type, file_or_headers):
    """
    Generates a validation report for the column headers in an OED input file
    or list or tuple of column headers.

    :param schema_type: The file schema type (``loc``, ``acc``, ``reinsinfo``
                        or ``reinsscope``).
    :type schema_type: str

    :param file_or_headers: An OED input file path or dict array of column
                            headers
    :type file_or_headers: str, list, tuple
    """
    errors = None
    try:
        for col_res, row_num, col_err in chain(
            (col_res, row_num, col_err) for col_res in OedValidator().validate_headers(schema_type, file_or_headers)
            for col_res, (row_num, col_err) in product([col_res], col_res['exceptions'])
            if col_res['pass'] is False
        ):
            errors = True
            line = (
                '{}:{}:{}: {}: {}\n'
                .format(
                    '{}'.format(file_or_headers) if isinstance(file_or_headers, str) else '',
                    row_num,
                    col_res['column_pos'],
                    col_err.msg,
                    col_err
                )
            )
            yield line
        if not errors:
            return
    except ProcessError as e:
        raise_with_traceback(ReportingError('Error while generating header validation report: {}'.format(e)))


def report_file(schema_type, file_or_data):
    """
    Generates a validation report for the column headers and data in an OED
    input file or list or tuple of column headers.

    :param schema_type: The file schema type (``loc``, ``acc``, ``reinsinfo`` or
                      ``reinsscope``).
    :type schema_type: str

    :param file_or_data: An OED input file path or dict array of rows
    :type file_or_data: str, list, tuple
    """
    errors = None
    try:
        for col_res, row_num, col_err in chain(
            (col_res, row_num, col_err) for col_res in OedValidator().validate(schema_type, file_or_data)[0]
            for col_res, (row_num, col_err) in product([col_res], col_res['exceptions'])
            if col_res['pass'] is False
        ):
            errors = True
            line = (
                '{}:{}:{}: {}: {}\n'
                .format(
                    '{}'.format(file_or_data) if isinstance(file_or_data, str) else '',
                    row_num,
                    col_res['column_pos'],
                    col_err.msg,
                    col_err
                )
            )
            yield line
        if not errors:
            return
    except ProcessError as e:
        raise_with_traceback(ReportingError('Error while generating validation report: {}'.format(e)))
