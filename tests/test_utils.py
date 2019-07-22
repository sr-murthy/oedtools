import builtins
import re
import string

from ast import literal_eval
from collections import Counter
from unittest import TestCase

import numpy as np

from hypothesis import (
    given,
    settings,
)
from hypothesis.strategies import (
    booleans,
    complex_numbers,
    floats,
    integers,
    just,
    lists,
    sampled_from,
    text,
)

from oedtools.utils import (
    generate_token_sequence,
    get_value,
    is_real_number,
    is_valid_token_sequence,
    sql_to_python_dtype,
    within_range,
)

from .data import SUPPORTED_SQL_DTYPES

class TestUtils(TestCase):

    @settings(max_examples=10)
    @given(sql_dtype=sampled_from(list(SUPPORTED_SQL_DTYPES)))
    def test_sql_to_python_dtype(self, sql_dtype):
        exp_py_dtype = SUPPORTED_SQL_DTYPES[sql_dtype]['py_dtype']
        if sql_dtype in ['char', 'varchar'] and not sql_dtype.endswith('(max)'):
            sql_dtype += '({})'.format(np.random.choice(range(1, 8001)))
        elif sql_dtype in ['nchar', 'nvarchar'] and not sql_dtype.endswith('(max)'):
            sql_dtype += '({})'.format(np.random.choice(range(1, 4001)))
        self.assertEqual(exp_py_dtype, sql_to_python_dtype(sql_dtype))

    @settings(max_examples=10)
    @given(sql_dtype=sampled_from(list(SUPPORTED_SQL_DTYPES)))
    def test_sql_to_python_dtype__as_numpy_dtype(self, sql_dtype):
        exp_py_dtype = SUPPORTED_SQL_DTYPES[sql_dtype]['numpy_dtype']
        if sql_dtype in ['char', 'varchar'] and not sql_dtype.endswith('(max)'):
            sql_dtype += '({})'.format(np.random.choice(range(1, 8001)))
        elif sql_dtype in ['nchar', 'nvarchar'] and not sql_dtype.endswith('(max)'):
            sql_dtype += '({})'.format(np.random.choice(range(1, 4001)))
        self.assertEqual(exp_py_dtype, sql_to_python_dtype(sql_dtype, as_numpy_dtype=True))

    @given(
        boo=booleans(),
        intg=integers(min_value=0),
        intgs=lists(integers(min_value=0), min_size=2, unique=True),
        flt=floats(min_value=0, allow_infinity=False),
        flt_bounds=lists(floats(min_value=0, allow_infinity=False), min_size=2, max_size=2, unique=True),
        comp=complex_numbers(allow_nan=False, allow_infinity=False),
        st=text(min_size=1, alphabet=(string.ascii_letters + string.digits + string.punctuation)),
        strs=lists(text(min_size=1, alphabet=(string.ascii_letters + string.digits + string.punctuation)), min_size=2, unique=True)
    )
    def test_within_range(self, boo, intg, intgs, flt, flt_bounds, comp, st, strs):
        res = within_range(intgs, intg)
        self.assertTrue(res) if intg in intgs else self.assertFalse(res)
        res = within_range(flt_bounds, flt)
        self.assertTrue(res) if flt >= min(flt_bounds) and flt <= max(flt_bounds) else self.assertFalse(res)
        res = within_range(strs, st)
        self.assertTrue(res) if st in strs else self.assertFalse(res)
        self.assertIsNone(within_range(intgs, None))
        self.assertIsNone(within_range(intgs, boo))
        self.assertIsNone(within_range(intgs, comp))
        self.assertIsNone(within_range(intgs, []))
        self.assertIsNone(within_range(intgs, tuple()))
        self.assertIsNone(within_range(intgs, dict()))
        self.assertIsNone(within_range(intgs, set()))

    @given(
        boo=booleans(),
        intg=integers(),
        flt=floats(allow_nan=False, allow_infinity=False),
        comp=complex_numbers(allow_nan=False, allow_infinity=False),
        st=text(min_size=1, alphabet=(string.ascii_letters + string.digits + string.punctuation))
    )
    def test_is_real_number(self, boo, intg, flt, comp, st):
        self.assertFalse(is_real_number(None))
        self.assertFalse(is_real_number(boo))
        self.assertTrue(is_real_number(intg))
        self.assertTrue(is_real_number(flt))
        self.assertFalse(is_real_number(comp))
        self.assertFalse(is_real_number(st))
        x = np.random.choice([None, boo, intg, flt, comp, st])
        self.assertFalse(is_real_number([x]))
        self.assertFalse(is_real_number((x,)))
        self.assertFalse(is_real_number({'key': x}))
        self.assertFalse(is_real_number({x}))

    @given(
        boo=booleans(),
        intg=integers(),
        flt=floats(allow_nan=False, allow_infinity=False),
        comp=complex_numbers(allow_nan=False, allow_infinity=False),
        st=text(min_size=1, alphabet=(string.ascii_letters + string.digits + string.punctuation))
    )
    def test_get_value(self, boo, intg, flt, comp, st):
        self.assertEqual(None, get_value(None))
        self.assertEqual(boo, get_value(boo))
        self.assertEqual(intg, get_value(str(intg)))
        self.assertEqual(flt, get_value(str(flt)))
        self.assertEqual(comp, get_value(str(comp)))
        qy = re.match(r'(-|\+){0,1}?(\d+)?(\.)?(\d+)?(e\+\d+|e-\d+)?(-|\+){0,1}?(\d+)?(\.)?(\d+)?(e\+\d+|e-\d+)?(j|J){0,1}?$', st)
        st_res = get_value(st)
        self.assertEqual(st, st_res) if not qy or isinstance(st_res, str) else self.assertNotEqual(st, st_res)
        x = np.random.choice([None, boo, intg, flt, comp, st])
        self.assertEqual((x,), get_value((x,)))
        self.assertEqual([x], get_value([x]))
        self.assertEqual({'key': x}, get_value({'key': x}))
        self.assertEqual({x}, get_value({x}))

    @given(
        token_alphabet=just(string.ascii_letters + string.digits),
        token_length=integers(min_value=2, max_value=10),
        num_tokens=integers(min_value=1, max_value=100),
        seq_length=integers(min_value=1, max_value=1000),
        sep=sampled_from(string.punctuation),
        unique=booleans()
    )
    def test_generate_token_sequence(self, token_alphabet, token_length, num_tokens, seq_length, sep, unique):
        tokens = sorted([
            ''.join(np.random.choice(list(token_alphabet), size=token_length).tolist())
            for i in range(num_tokens)
        ])
        seq = generate_token_sequence(tokens, seq_length, sep, unique=unique)
        chars_ex_sep = [char for char in string.punctuation if char != sep]
        self.assertTrue(not any(char in sep for char in chars_ex_sep))
        res_tokens = sorted(seq.split(sep))
        self.assertTrue(set(res_tokens).issubset(tokens))
        if unique is True:
            self.assertEqual(len(res_tokens), min(num_tokens, seq_length))
        else:
            self.assertEqual(len(res_tokens), seq_length)
        self.assertTrue(all(len(token) == token_length) for token in res_tokens)

    def test_is_valid_token_sequence__with_non_string_token_sequence__returns_null(self):
        self.assertIsNone(is_valid_token_sequence([], 1))
        self.assertIsNone(is_valid_token_sequence([], True))
        self.assertIsNone(is_valid_token_sequence([], -1.0))
        self.assertIsNone(is_valid_token_sequence([], complex(-1.0)))
        self.assertIsNone(is_valid_token_sequence([], [1]))
        self.assertIsNone(is_valid_token_sequence([], (1,)))
        self.assertIsNone(is_valid_token_sequence([], {1}))
        self.assertIsNone(is_valid_token_sequence([], {'a': 1}))
        self.assertIsNone(is_valid_token_sequence([], range(1)))
        self.assertIsNone(is_valid_token_sequence([], int))

    def test_is_valid_token_sequence__with_some_non_string_or_empty_string_fixed_tokens__returns_null(self):
        self.assertIsNone(is_valid_token_sequence(['token', ''], 'seq'))
        self.assertIsNone(is_valid_token_sequence(['token', 1], 'seq'))
        self.assertIsNone(is_valid_token_sequence(['token', True], 'seq'))
        self.assertIsNone(is_valid_token_sequence(['token', -1.0], 'seq'))
        self.assertIsNone(is_valid_token_sequence(['token', complex(-1.0)], 'seq'))
        self.assertIsNone(is_valid_token_sequence(['token', [1]], 'seq'))
        self.assertIsNone(is_valid_token_sequence(['token', (1,)], 'seq'))
        self.assertIsNone(is_valid_token_sequence(['token', {1}], 'seq'))
        self.assertIsNone(is_valid_token_sequence(['token', {'a': 1}], 'seq'))
        self.assertIsNone(is_valid_token_sequence(['token', range(1)], 'seq'))
        self.assertIsNone(is_valid_token_sequence(['token', int], 'seq'))

    @settings(max_examples=10)
    @given(
        token_alphabet=just(string.ascii_letters + string.digits),
        token_length=integers(min_value=2, max_value=10),
        num_tokens=integers(min_value=3, max_value=20),
        token_seq_sep=sampled_from([';', ':', '-', ',', '.', '|', '/', '\\']),
        terminate_token_seq_with_sep=booleans()
    )
    def test_is_valid_token_sequence__with_invalid_token_sequence_containing_duplicate_token__returns_false(
        self,
        token_alphabet,
        token_length,
        num_tokens,
        token_seq_sep,
        terminate_token_seq_with_sep
    ):
        tokens = [
            ''.join(np.random.choice(list(token_alphabet), size=token_length).tolist())
            for i in range(num_tokens)
        ]
        token_seq_len = np.random.choice(range(1, num_tokens + 1))
        token_seq = '{}'.format(token_seq_sep).join(
            np.random.choice(tokens, size=token_seq_len, replace=False)
        )
        token_seq += token_seq_sep + [t for t in tokens if t in token_seq][0]
        if terminate_token_seq_with_sep:
            token_seq += token_seq_sep

        self.assertFalse(is_valid_token_sequence(tokens, token_seq, token_seq_sep))

    @settings(max_examples=10)
    @given(
        token_alphabet=just(string.ascii_letters + string.digits),
        token_length=integers(min_value=2, max_value=10),
        num_tokens=integers(min_value=3, max_value=20),
        token_seq_sep=sampled_from([';', ':', '-', ',', '.', '|', '/', '\\']),
        terminate_token_seq_with_sep=booleans()
    )
    def test_is_valid_token_sequence__with_valid_token_sequence__returns_true(
        self,
        token_alphabet,
        token_length,
        num_tokens,
        token_seq_sep,
        terminate_token_seq_with_sep
    ):
        tokens = [
            ''.join(np.random.choice(list(token_alphabet), size=token_length).tolist())
            for i in range(num_tokens)
        ]
        token_seq_len = np.random.choice(range(1, num_tokens + 1))
        token_seq = '{}'.format(token_seq_sep).join(np.random.choice(tokens, size=token_seq_len, replace=False))
        if terminate_token_seq_with_sep:
            token_seq += token_seq_sep

        self.assertTrue(is_valid_token_sequence(tokens, token_seq, token_seq_sep))
