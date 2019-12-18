__all__ = [
    'generate_token_sequence',
    'get_method',
    'get_value',
    'is_real_number',
    'is_valid_token_sequence',
    'SQL_NUMERIC_DTYPES',
    'sql_to_python_dtype',
    'within_range'
]


"""
Package utilities
"""
import importlib
import re

from collections import (
    Counter,
    OrderedDict,
)

import numpy as np


SQL_NUMERIC_DTYPES = OrderedDict({
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
    }
})


def sql_to_python_dtype(sql_dtype, as_numpy_dtype=False):
    """
    Converts an SQL datatype string to a Python datatype
    string, or optionally a Numpy datatype string.

    :param sql_dtype: SQL datatype string
    :type sql_dtype: str

    :param as_numpy_dtype: (Optional) Numpy datatype string conversion option
    :type as_numpy_dtype: bool

    :return: A Python or Numpy datatype string
    :rtype: str
    """
    _sql_dtype = ''.join(sql_dtype.lower())

    if _sql_dtype in ['unsigned bit', 'bit', '0 or 1']:
        return 'int' if not as_numpy_dtype else 'uint8'

    q = re.match(r'(unsigned )?(tiny|small|big)?int', _sql_dtype)
    if q and q.groups()[1] == 'tiny':
        return 'int' if not as_numpy_dtype else ('uint8' if q.groups()[0] and q.groups()[0].strip() == 'unsigned' else 'int8')
    elif q and q.groups()[1] == 'small':
        return 'int' if not as_numpy_dtype else ('uint16' if q.groups()[0] and q.groups()[0].strip() == 'unsigned' else 'int16')
    elif q and not q.groups()[1]:
        return 'int' if not as_numpy_dtype else ('uint32' if q.groups()[0] and q.groups()[0].strip() == 'unsigned' else 'int32')
    elif q and q.groups()[1] == 'big':
        return 'int' if not as_numpy_dtype else ('uint64' if q.groups()[0] and q.groups()[0].strip() == 'unsigned' else 'int64')

    if _sql_dtype == 'real':
        return 'float' if not as_numpy_dtype else 'float32'

    if _sql_dtype in ['float', 'decimal', 'numeric']:
        return 'float' if not as_numpy_dtype else 'float64'

    if re.match(r'(n)?(var)?char(\(\d+|max\))?', _sql_dtype):
        return 'str' if not as_numpy_dtype else 'object'

    if re.match(r'((date(time)?)?|(time(stamp)?)?|(year)?){1}$', _sql_dtype):
        return 'str' if not as_numpy_dtype else 'object'

    return 'str' if not as_numpy_dtype else 'object'


def generate_token_sequence(tokens, seq_length=10, sep=';', unique=True):
    """
    Generates a string containing a sequence of tokens, randomly sampled from
    a given list or tuple of tokens, of a given length and separated by a given
    separator. Uniqueness of tokens in the sequence string holds by default,
    but can be turned off by setting ``unique`` to ``False``.

    :param tokens: The list or tuple of tokens to sample from
    :type tokens: list, tuple

    :param seq_length: The length of the token sequence (and therefore the
                       number of tokens in the string) - set to the minimum
                       of the size of the token set and the requested length
                       of the token sequence (default is ``10``)
    :type seq_length: int

    :param sep: The token separator - usually a single non-text character
                (default is ``;``)
    :type sep: str

    :param unique: Whether the tokens in the sequence must be unique (default
                   is ``True``)
    :type unique: bool

    :return: The token sequence as a ``sep``-separated string
    :rtype: str
    """
    return '{}'.format(sep).join(
                sorted(
                    np.random.choice(
                        tokens,
                        size=min(seq_length, len(tokens)) if unique else seq_length,
                        replace=(not unique)
                    ).tolist()
                )
            )


def is_valid_token_sequence(tokens, seq, sep=';'):
    """
    Checks whether a string consists of a sequence of unique tokens from a
    fixed set of tokens, separated by a given separator. It is used to check
    the correctness of values in several OED columns such as
    ``(Acc|Cond|Loc|Pol|Reins)Peril`` and also
    ``(Acc|Cond|Loc|Pol|Reins)PerilsCovered``, which must be ``;``-separated
    sequence of OED peril codes, e.g. ``AA1;WTC;WEC``.

    :param tokens: The iterable of tokens to check the string tokens against
    :type tokens: list, tuple, set

    :param seq: The string to be checked
    :type seq: str

    :param sep: (Optional) The separator to use/expect - default is ``;``
    :type sep: str

    :return: Whether the string is valid
    :rtype: bool
    """
    if not isinstance(seq, str) or any(not isinstance(t, str) for t in tokens) or any(len(t) == 0 for t in tokens):
        return

    seq_tokens = [t for t in seq.split(sep) if t]
    seq_tokens_cntr = Counter(seq_tokens).values()
    return not (
        any(t not in tokens for t in seq_tokens) or
        any(v > 1 for v in seq_tokens_cntr)
    )


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


def within_range(bounds_or_iter, val):
    """
    Checks whether a given simple literal value (integer, float, string, bytes)
    lies within a given finite set of values or a bounded numeric range
    (or interval).

    :param bounds: A finite set of values or bounded numeric range
                   (or interval)
    :type bounds: tuple, list, set, range

    :param val: The value to be checked
    :type val: int, float, str, bytes

    :return: Status of range check
    :rtype: bool
    """
    if is_real_number(val):
        return (
            val in bounds_or_iter if isinstance(bounds_or_iter, range)
            else (
                val in bounds_or_iter if isinstance(val, int)
                else val >= min(bounds_or_iter) and val <= max(bounds_or_iter)
            )
        )
    elif isinstance(val, str) or isinstance(val, bytes):
        return val in bounds_or_iter

    return


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
