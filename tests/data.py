__all__ = [
    'ACC',
    'ACC_NONNULL',
    'ACC_OPTIONAL',
    'ACC_REQUIRED',
    'ALL',
    'DEFAULTS',
    'FLOAT',
    'get_method',
    'get_value',
    'GROUPED_SCHEMA',
    'INT',
    'is_real_number',
    'LOC',
    'LOC_NONNULL',
    'LOC_OPTIONAL',
    'LOC_REQUIRED',
    'NONNULL',
    'NUMERIC',
    'NUMPY_DTYPES',
    'OPTIONAL',
    'PYTHON_DTYPES',
    'REINSINFO',
    'REINSINFO_NONNULL',
    'REINSINFO_OPTIONAL',
    'REINSINFO_REQUIRED',
    'REINSSCOPE',
    'REINSSCOPE_NONNULL',
    'REINSSCOPE_OPTIONAL',
    'REINSSCOPE_REQUIRED',
    'REQUIRED',
    'REQUIRED_TYPES',
    'REQUIRED_NONNULL',
    'sample_column',
    'SCHEMA_TYPES',
    'SCHEMA_TYPES_EX_MASTER',
    'SQL_DTYPES',
    'STRING_WITH_FINITE_RANGE',
    'SUPPORTED_SQL_DTYPES',
    'VALUE_GROUPS'
]

import builtins
import importlib
import json
import string

from json import JSONDecodeError

import numpy as np

from hypothesis.strategies import (
    booleans,
    fixed_dictionaries,
    floats,
    integers,
    lists,
    text,
)

from oedtools.schema import (
    get_column_schema,
    get_grouped_master_schema,
    get_schema,
    SCHEMA_DIR,
)
from oedtools.values import (
    get_values_profile,
)

MASTER_SCHEMA = get_schema()

GROUPED_SCHEMA = get_grouped_master_schema()

SCHEMA_TYPES = list(GROUPED_SCHEMA)
SCHEMA_TYPES_EX_MASTER = list(set(SCHEMA_TYPES).difference(['master']))

VALUE_GROUPS = sorted(get_values_profile())

ALL = sorted(MASTER_SCHEMA)
REQUIRED_TYPES = ['R', 'CR', 'O']
REQUIRED = sorted(k for k, v in MASTER_SCHEMA.items() if v['required'] == 'R')
OPTIONAL = sorted(set(ALL).difference(REQUIRED))
NONNULL = sorted(k for k, v in MASTER_SCHEMA.items() if not v['blank'])
REQUIRED_NONNULL = set(REQUIRED).intersection(NONNULL)
NUMERIC = sorted(k for k, v in MASTER_SCHEMA.items() if v['py_dtype'] in ['int', 'float'])
INT = sorted(k for k, v in MASTER_SCHEMA.items() if v['py_dtype'] == 'int')
FLOAT = sorted(k for k, v in MASTER_SCHEMA.items() if v['py_dtype'] == 'float')
DEFAULTS = list(set([v['default'] for v in MASTER_SCHEMA.values() if v['default'] is not None]))
PYTHON_DTYPES = list(set([v['py_dtype'] for v in MASTER_SCHEMA.values()]))
SQL_DTYPES = list(set([v['sql_dtype'] for v in MASTER_SCHEMA.values()]))
NUMPY_DTYPES = list(set([v['numpy_dtype'] for v in MASTER_SCHEMA.values()]))
STRING_WITH_FINITE_RANGE = sorted(k for k, v in MASTER_SCHEMA.items() if v['py_dtype'] == 'str' and v['column_range'] is not None)
LOC = sorted(k[1] for k, v in MASTER_SCHEMA.items() if v['entity'].lower() == 'loc')
LOC_REQUIRED = sorted([k for k in set(LOC).intersection([v for k, v in REQUIRED if k == 'loc'])])
LOC_NONNULL = sorted([k for k in set(LOC).intersection([v for k, v in NONNULL if k == 'loc'])])
LOC_OPTIONAL = sorted([k for k in set(LOC).intersection([v for k, v in OPTIONAL if k == 'loc'])])
ACC = sorted(k[1] for k, v in MASTER_SCHEMA.items() if v['entity'].lower() == 'acc')
ACC_REQUIRED = sorted([k for k in set(ACC).intersection([v for k, v in REQUIRED if k == 'acc'])])
ACC_NONNULL = sorted([k for k in set(ACC).intersection([v for k, v in NONNULL if k == 'acc'])])
ACC_OPTIONAL = sorted([k for k in set(ACC).intersection([v for k, v in OPTIONAL if k == 'acc'])])
REINSINFO = sorted(k[1] for k, v in MASTER_SCHEMA.items() if v['entity'].lower() == 'reinsinfo')
REINSINFO_REQUIRED = sorted([k for k in set(REINSINFO).intersection([v for k, v in REQUIRED if k == 'reinsinfo'])])
REINSINFO_NONNULL = sorted([k for k in set(REINSINFO).intersection([v for k, v in NONNULL if k == 'reinsinfo'])])
REINSINFO_OPTIONAL = sorted([k for k in set(REINSINFO).intersection([v for k, v in OPTIONAL if k == 'reinsinfo'])])
REINSSCOPE = sorted(k[1] for k, v in MASTER_SCHEMA.items() if v['entity'].lower() == 'reinsscope')
REINSSCOPE_REQUIRED = sorted([k for k in set(REINSSCOPE).intersection([v for k, v in REQUIRED if k == 'reinsscope'])])
REINSSCOPE_NONNULL = sorted([k for k in set(REINSSCOPE).intersection([v for k, v in NONNULL if k == 'reinsscope'])])
REINSSCOPE_OPTIONAL = sorted([k for k in set(REINSSCOPE).intersection([v for k, v in OPTIONAL if k == 'reinsscope'])])

SUPPORTED_SQL_DTYPES = {
    'bit': {
        'bits': 1, 'signed': False, 'range': range(2), 'py_dtype': 'int', 'numpy_dtype': 'uint8'
    },
    'unsigned bit': {
        'bits': 1, 'signed': False, 'range': range(2), 'py_dtype': 'int', 'numpy_dtype': 'uint8'
    },
    'tinyint': {
        'bits': 8, 'signed': True, 'range': range(-2 ** 7, 2 ** 7 + 1), 'py_dtype': 'int', 'numpy_dtype': 'int8'
    },
    'unsigned tinyint': {
        'bits': 8, 'signed': False, 'range': range(2 ** 8), 'py_dtype': 'int', 'numpy_dtype': 'uint8'
    },
    'smallint': {
        'bits': 16, 'signed': True, 'range': range(-2 ** 15, 2 ** 15 - 1), 'py_dtype': 'int', 'numpy_dtype': 'int16'
    },
    'unsigned smallint': {
        'bits': 16, 'signed': False, 'range': range(2 ** 16), 'py_dtype': 'int', 'numpy_dtype': 'uint16'
    },
    'int': {
        'bits': 32, 'signed': True, 'range': range(-2 ** 31, 2 ** 31 - 1), 'py_dtype': 'int', 'numpy_dtype': 'int32'
    },
    'unsigned int': {
        'bits': 32, 'signed': False, 'range': range(2 ** 32), 'py_dtype': 'int', 'numpy_dtype': 'uint32'
    },
    'bigint': {
        'bits': 64, 'signed': True, 'range': range(-2 ** 63, 2 ** 63 - 1), 'py_dtype': 'int', 'numpy_dtype': 'int64'
    },
    'unsigned bigint': {
        'bits': 64, 'signed': False, 'range': range(2 ** 64), 'py_dtype': 'int', 'numpy_dtype': 'uint64'
    },
    'unsigned bigint': {
        'bits': 64, 'signed': False, 'range': range(2 ** 64), 'py_dtype': 'int', 'numpy_dtype': 'uint64'
    },
    'float': {
        'bits': 64, 'signed': True, 'range': (-1.79e+308, 1.79e+308), 'py_dtype': 'float', 'numpy_dtype': 'float64'
    },
    'real': {
        'bits': 32, 'signed': True, 'range': (-3.40e+38, 3.40e+38), 'py_dtype': 'float', 'numpy_dtype': 'float32'
    },
    'decimal': {
        'bits': 'variable', 'signed': True, 'range': (-10e+38 + 1, 10e38 - 1), 'py_dtype': 'float', 'numpy_dtype': 'float64'
    },
    'numeric': {
        'bits': 'variable', 'signed': True, 'range': (-10e+38 + 1, 10e38 - 1), 'py_dtype': 'float', 'numpy_dtype': 'float64'
    },
    'char': {
        'bits': 'variable', 'signed': None, 'range': None, 'py_dtype': 'str', 'numpy_dtype': 'object'
    },
    'nchar': {
        'bits': 'variable', 'signed': None, 'range': None, 'py_dtype': 'str', 'numpy_dtype': 'object'
    },
    'varchar': {
        'bits': 'variable', 'signed': None, 'range': None, 'py_dtype': 'str', 'numpy_dtype': 'object'
    },
    'varchar(max)': {
        'bits': 'variable', 'signed': None, 'range': None, 'py_dtype': 'str', 'numpy_dtype': 'object'
    },
    'nvarchar(max)': {
        'bits': 'variable', 'signed': None, 'range': None, 'py_dtype': 'str', 'numpy_dtype': 'object'
    },
    'date': {
        'bits': 'variable', 'signed': None, 'range': None, 'py_dtype': 'str', 'numpy_dtype': 'object'
    },
    'datetime': {
        'bits': 'variable', 'signed': None, 'range': None, 'py_dtype': 'str', 'numpy_dtype': 'object'
    },
    'time': {
        'bits': 'variable', 'signed': None, 'range': None, 'py_dtype': 'str', 'numpy_dtype': 'object'
    },
    'timestamp': {
        'bits': 'variable', 'signed': None, 'range': None, 'py_dtype': 'str', 'numpy_dtype': 'object'
    },
    'year': {
        'bits': 'variable', 'signed': None, 'range': None, 'py_dtype': 'str', 'numpy_dtype': 'object'
    }
}

def get_column_schema(schema_type, header):
    """
    Gets the column schema (definition) for a given column in an OED acc.,
    loc., reins. info. or reins. scope file, using a grouped version of
    the master file schema.

    :param schema_type: OED schema type indicator (``master``, ``loc``,
                        ``acc``, ``reinsinfo``, or ``reinsscope``)
    :type schema_type: str

    :param header: The column header
    :type column: str

    :return: The column schema
    :rtype: dict
    """
    _schema_type = schema_type.lower()

    if _schema_type == 'master':
        raise OedError(
            'Column schemas are only available for specific file '
            'schema types - "acc", "loc", "reinsinfo" or "reinsscope"'
        )

    schema = get_grouped_master_schema()

    _header = header.lower()

    try:
        col_schema = schema[_schema_type][_header]
    except KeyError:
        is_non_oed_schema = _schema_type not in [stype for stype in schema]
        is_non_oed_column = not any(_header in schema[stype] for stype in schema)

        if is_non_oed_schema and is_non_oed_column:
            raise get_file_error(
                'non oed schema and column',
                '"{}" is not a valid OED schema type and "{}" is not a valid '
                'column in any OED schema'.format(schema_type, header)
            )
        elif is_non_oed_schema and not is_non_oed_column:
            raise get_file_error(
                'non oed schema',
                '"{}" is not a valid OED schema type'.format(schema_type)
            )
        elif not is_non_oed_schema and is_non_oed_column:
            raise get_file_error(
                'non oed column',
                '"{}" is not a valid column in any OED schema'.format(header)
            )
        elif not is_non_oed_schema and _header not in schema[_schema_type]:
            raise get_file_error(
                'non oed schema column',
                '"{}" is not a valid column in the OED "{}" schema'.format(header, schema_type)
            )
    else:
        return col_schema


def update_schemas():
    """
    A method to automatically re-generate the values profile JSON, and also
    all the file schemas, in ``oedtools/schema/``.
    """
    generate_values_profile(os.path.join(SCHEMA_DIR, 'values.json'))
    for schema_type in ['master', 'loc', 'acc', 'reinsinfo', 'reinsscope']:
        generate_schema(
            os.path.join(SCHEMA_DIR, '{}_def.csv'.format(schema_type)),
            os.path.join(SCHEMA_DIR, '{}_schema.json'.format(schema_type))
        )


def get_method(pkg_path):
    """
    Returns a method given the full package path of the method, e.g.
    given the string ``oedtools.schema.get_schema`` it will return the
    actual method ``get_schema`` from the ``oedtools.schema`` module.

    :param pkg_path: Full package path of the method
    :type pkg_path: str

    :return: The actual method
    :rtype: function
    """
    path_tokens = pkg_path.split('.')
    return getattr(importlib.import_module('.'.join(path_tokens[:-1])), path_tokens[-1])


def is_real_number(val):
    """
    Simple method to check whether a literal value is a real number - returns
    ``True`` for integers as well.

    :param num: The literal value, which can be any literal value
    :type num: None, bool, int, float, complex, str, bytes, tuple, list, dict, set

    :return: Whether the value is a real number
    :rtype: bool
    """
    if isinstance(val, bool) or not (isinstance(val, int) or isinstance(val, float)):
        return False

    return True


def get_value(val):
    """
    Returns the number from a literal value if the value represents either an
    integer, real number or complex number. The use case is to extract numbers
    from string literals, if those literals are instances of one of three
    proper numeric types (``int``, ``float``, ``complex``). All other types
    of string are returned unchanged.

    :param val: The literal value, which can be any literal value
    :type val: None, bool, int, float, compex, str, bytes, tuple, list, dict, set

    :return: The number represented by the literal, if it represents a number,
             or the literal
    :rtype: None, bool, int, float, compex, str, bytes, tuple, list, dict, set
    """
    if val is None or isinstance(val, bool) or isinstance(val, bytes):
        return val
    if is_real_number(val) or isinstance(val, complex):
        return val
    try:
        return int(val)
    except (TypeError, ValueError):
        try:
            return float(val)
        except (TypeError, ValueError):
            try:
                return complex(val)
            except (TypeError, ValueError):
                return val


def sample_column(schema_type, header, str_width=None, size=10):
    """
    Sampling values in a given column in a given schema (``acc``, ``loc``,
    ``reinsinfo``, ``reinsscope``), consistent with the validation method
    for the column (if there is one), or with the column range (if a column
    range is defined), or with the data type range.

    :param schema_type: OED schema type indicator (``master``, ``loc``,
                        ``acc``, ``reinsinfo``, or ``reinsscope``)
    :type schema_type: str

    :param header: The column header
    :type column: str

    :param str_width: Optional argument applicable only to string type columns
                      with no defined column range and/or validation method. If
                      applicable this option sets a fixed width for the
                      individual string values sampled for the column
    :type str_width: int

    :param size: Number of values to sample
    :type size: int

    :return: Sampled values
    :rtype: list
    """
    if size <= 0:
        size = 10

    col_schema = get_column_schema(schema_type, header)

    if col_schema['py_dtype'] is None:
        return

    py_dtype = getattr(builtins, col_schema['py_dtype'])

    dtype_range = col_schema['dtype_range']
    column_range = col_schema['column_range']

    use_range = column_range or dtype_range

    try:
        sampling_info = json.loads(col_schema['column_sampling'])
    except (JSONDecodeError, TypeError, ValueError):
        sampling_info = sampling_func = None
    else:
        sampling_func = get_method(sampling_info['func'])

    if py_dtype in [int, float, str] and column_range is not None and sampling_func is not None:
        return [
            sampling_func(column_range, *sampling_info['args'][1:])
            for i in range(size)
        ]
    elif py_dtype is int:
        return (
            np.random.randint(use_range.start, use_range.stop, size=size).tolist() if isinstance(use_range, range)
            else np.random.choice(use_range, size=size).tolist()
        )
    elif py_dtype is float:
        return (
            np.random.uniform(max(min(use_range), -1.79e+307), min(max(use_range), +1.79e+307), size=size).tolist()
        )
    elif py_dtype is str and column_range is not None and sampling_func is None:
        return [
            np.random.choice(column_range)
            for i in range(size)
        ]
    elif py_dtype is str and column_range is None:
        return [
            ''.join(np.random.choice(list(string.ascii_letters + string.digits), size=(str_width or 20)))
            for i in range(size)
        ]
