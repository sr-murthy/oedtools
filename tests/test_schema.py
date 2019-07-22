import builtins
import io
import json
import os
import re
import string
import sys
import time

from ast import literal_eval
from collections import OrderedDict
from datetime import datetime
from itertools import groupby
from json import JSONDecodeError
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
    just,
    sampled_from,
    text,
)

from oedtools.exceptions import (
    OedError,
    NonOedColumnError,
    NonOedSchemaColumnError,
    NonOedSchemaAndColumnError,
    NonOedSchemaError,
)
from oedtools.schema import (
    generate_schema,
    get_column_schema,
    get_grouped_master_schema,
    get_schema,
    get_schema_version,
    get_values_profile,
    sample_column,
    SCHEMA_DIR,
    update_schemas,
)

from .data import (
    ACC,
    ALL,
    get_method,
    LOC,
    REINSINFO,
    REINSSCOPE,
    SCHEMA_TYPES,
    SCHEMA_TYPES_EX_MASTER,
    VALUE_GROUPS,
)


class TestSchema(TestCase):

    def setUp(self):
        self.SCHEMA_DIR = SCHEMA_DIR

        self.schema_version_fp = os.path.join(self.SCHEMA_DIR, 'schema_version.txt')
        
        self.values_csv_fp = os.path.join(self.SCHEMA_DIR, 'values.csv')
        self.values_json_fp = os.path.join(self.SCHEMA_DIR, 'values.json')
        self.values_profile = get_values_profile()
        
        self.master_csv_schema_fp = os.path.join(self.SCHEMA_DIR, 'master_def.csv')
        self.master_json_schema_fp = os.path.join(self.SCHEMA_DIR, 'master_schema.json')
        self.master_schema = get_schema()

        self.loc_csv_schema_fp = os.path.join(self.SCHEMA_DIR, 'loc_def.csv')
        self.loc_json_schema_fp = os.path.join(self.SCHEMA_DIR, 'loc_schema.json')
        self.loc_schema = get_schema('loc')
        
        self.acc_csv_schema_fp = os.path.join(self.SCHEMA_DIR, 'acc_def.csv')
        self.acc_json_schema_fp = os.path.join(self.SCHEMA_DIR, 'acc_schema.json')
        self.acc_schema = get_schema('acc')

        self.reinsinfo_csv_schema_fp = os.path.join(self.SCHEMA_DIR, 'reinsinfo_def.csv')
        self.reinsinfo_json_schema_fp = os.path.join(self.SCHEMA_DIR, 'reinsinfo_schema.json')
        self.reinsinfo_schema = get_schema('reinsinfo')
        
        self.reinsscope_csv_schema_fp = os.path.join(self.SCHEMA_DIR, 'reinsscope_def.csv')
        self.reinsscope_json_schema_fp = os.path.join(self.SCHEMA_DIR, 'reinsscope_schema.json')
        self.reinsscope_schema = get_schema('reinsscope')

        self.csv_to_json_col_map = {
            'backend_db_field_name': 'oed_db_field_name',
            'backend_table': 'oed_db_table',
            'blank': 'blank',
            'default': 'default',
            'desc': 'desc',
            'entity': 'entity',
            'field_name': 'field_name',
            'required': 'required',
            'secmod': 'secmod',
            'type': 'sql_dtype'
        }

        self.json_to_csv_type_col_map = {
            'unsigned bit': '0 or 1',
            'unsigned tinyint': 'tinyint',
            'unsigned smallint': 'smallint',
            'unsigned int': 'int',
            'unsigned bigint': 'bigint'
        }

        self.json_to_csv_blank_col_map = {
            False: 'NO',
            True: 'YES'
        }

    def test_generate_schema__master(self):
        master_df = pd.read_csv(self.master_csv_schema_fp)
        master_df.columns = master_df.columns.str.lower()
        master_df = master_df.where(master_df.notnull(), None)
        master_df = master_df.sort_values(['entity', 'field_name']).reset_index(drop=True)

        with NamedTemporaryFile('w') as target_json:
            generate_schema(self.master_csv_schema_fp, target_json.name)
            with io.open(os.path.join('oedtools', 'schema', 'master_schema.json'), 'r', encoding='utf-8') as f: 
                res_schema = {literal_eval(k): v for k, v in json.load(f).items()}

            for exp_key, exp_it in (((it['entity'].lower(), it['field_name'].lower()), it) for _, it in master_df.iterrows()):
                self.assertIn(exp_key, res_schema)
                res_key = exp_key
                res_it = res_schema[res_key]
                self.assertIsInstance(res_it, dict)
                self.assertEqual(exp_it['backend_db_field_name'], res_it[self.csv_to_json_col_map['backend_db_field_name']])
                self.assertEqual(exp_it['backend_table'], res_it[self.csv_to_json_col_map['backend_table']])
                self.assertEqual(exp_it['blank'], self.json_to_csv_blank_col_map[res_it[self.csv_to_json_col_map['blank']]])
                if res_it['py_dtype'] is not None:
                    self.assertEqual(
                        getattr(builtins, res_it['py_dtype'])(exp_it['default'] if res_it['py_dtype'] != 'bool' else int(exp_it['default'])) if exp_it['default'] is not None else None,
                        res_it[self.csv_to_json_col_map['default']]
                    )
                self.assertEqual(exp_it['desc'], res_it[self.csv_to_json_col_map['desc']])
                self.assertEqual(exp_it['entity'], res_it[self.csv_to_json_col_map['entity']])
                self.assertEqual(exp_it['field_name'], res_it[self.csv_to_json_col_map['field_name']])
                self.assertEqual(exp_it['required'], res_it[self.csv_to_json_col_map['required']])
                self.assertEqual(
                    exp_it['type'],
                    (
                        self.json_to_csv_type_col_map[res_it['sql_dtype']]
                        if res_it['sql_dtype'].endswith('int') or res_it['sql_dtype'].endswith('bit')
                        else res_it['sql_dtype']
                    )
                )

    def test_generate_schema__loc(self):
        loc_df = pd.read_csv(self.loc_csv_schema_fp)
        loc_df.columns = loc_df.columns.str.lower()
        loc_df = loc_df.where(loc_df.notnull(), None)
        loc_df = loc_df.sort_values(['entity', 'field_name']).reset_index(drop=True)

        with NamedTemporaryFile('w') as target_json:
            generate_schema(self.loc_csv_schema_fp, target_json.name)
            with io.open(os.path.join('oedtools', 'schema', 'loc_schema.json'), 'r', encoding='utf-8') as f: 
                res_schema = {literal_eval(k): v for k, v in json.load(f).items()}

            for exp_key, exp_it in (((it['entity'].lower(), it['field_name'].lower()), it) for _, it in loc_df.iterrows()):
                self.assertIn(exp_key, res_schema)
                res_key = exp_key
                res_it = res_schema[res_key]
                self.assertIsInstance(res_it, dict)
                self.assertEqual(exp_it['backend_db_field_name'], res_it[self.csv_to_json_col_map['backend_db_field_name']])
                self.assertEqual(exp_it['backend_table'], res_it[self.csv_to_json_col_map['backend_table']])
                self.assertEqual(exp_it['blank'], self.json_to_csv_blank_col_map[res_it[self.csv_to_json_col_map['blank']]])
                if res_it['py_dtype'] is not None:
                    self.assertEqual(
                        getattr(builtins, res_it['py_dtype'])(exp_it['default'] if res_it['py_dtype'] != 'bool' else int(exp_it['default'])) if exp_it['default'] is not None else None,
                        res_it[self.csv_to_json_col_map['default']]
                    )
                self.assertEqual(exp_it['desc'], res_it[self.csv_to_json_col_map['desc']])
                self.assertEqual(exp_it['entity'], res_it[self.csv_to_json_col_map['entity']])
                self.assertEqual(exp_it['field_name'], res_it[self.csv_to_json_col_map['field_name']])
                self.assertEqual(exp_it['required'], res_it[self.csv_to_json_col_map['required']])
                self.assertEqual(
                    exp_it['type'],
                    (
                        self.json_to_csv_type_col_map[res_it['sql_dtype']]
                        if res_it['sql_dtype'].endswith('int') or res_it['sql_dtype'].endswith('bit')
                        else res_it['sql_dtype']
                    )
                )

    def test_generate_schema__acc(self):
        acc_df = pd.read_csv(self.acc_csv_schema_fp)
        acc_df.columns = acc_df.columns.str.lower()
        acc_df = acc_df.where(acc_df.notnull(), None)
        acc_df = acc_df.sort_values(['entity', 'field_name']).reset_index(drop=True)

        with NamedTemporaryFile('w') as target_json:
            generate_schema(self.acc_csv_schema_fp, target_json.name)
            with io.open(os.path.join('oedtools', 'schema', 'acc_schema.json'), 'r', encoding='utf-8') as f: 
                res_schema = {literal_eval(k): v for k, v in json.load(f).items()}

            for exp_key, exp_it in (((it['entity'].lower(), it['field_name'].lower()), it) for _, it in acc_df.iterrows()):
                self.assertIn(exp_key, res_schema)
                res_key = exp_key
                res_it = res_schema[res_key]
                self.assertIsInstance(res_it, dict)
                self.assertEqual(exp_it['backend_db_field_name'], res_it[self.csv_to_json_col_map['backend_db_field_name']])
                self.assertEqual(exp_it['backend_table'], res_it[self.csv_to_json_col_map['backend_table']])
                self.assertEqual(exp_it['blank'], self.json_to_csv_blank_col_map[res_it[self.csv_to_json_col_map['blank']]])
                if res_it['py_dtype'] is not None:
                    self.assertEqual(
                        getattr(builtins, res_it['py_dtype'])(exp_it['default'] if res_it['py_dtype'] != 'bool' else int(exp_it['default'])) if exp_it['default'] is not None else None,
                        res_it[self.csv_to_json_col_map['default']]
                    )
                self.assertEqual(exp_it['desc'], res_it[self.csv_to_json_col_map['desc']])
                self.assertEqual(exp_it['entity'], res_it[self.csv_to_json_col_map['entity']])
                self.assertEqual(exp_it['field_name'], res_it[self.csv_to_json_col_map['field_name']])
                self.assertEqual(exp_it['required'], res_it[self.csv_to_json_col_map['required']])
                self.assertEqual(
                    exp_it['type'],
                    (
                        self.json_to_csv_type_col_map[res_it['sql_dtype']]
                        if res_it['sql_dtype'].endswith('int') or res_it['sql_dtype'].endswith('bit')
                        else res_it['sql_dtype']
                    )
                )

    def test_generate_schema__reinsinfo(self):
        reinsinfo_df = pd.read_csv(self.reinsinfo_csv_schema_fp)
        reinsinfo_df.columns = reinsinfo_df.columns.str.lower()
        reinsinfo_df = reinsinfo_df.where(reinsinfo_df.notnull(), None)
        reinsinfo_df = reinsinfo_df.sort_values(['entity', 'field_name']).reset_index(drop=True)

        with NamedTemporaryFile('w') as target_json:
            generate_schema(self.reinsinfo_csv_schema_fp, target_json.name)
            with io.open(os.path.join('oedtools', 'schema', 'reinsinfo_schema.json'), 'r', encoding='utf-8') as f: 
                res_schema = {literal_eval(k): v for k, v in json.load(f).items()}

            for exp_key, exp_it in (((it['entity'].lower(), it['field_name'].lower()), it) for _, it in reinsinfo_df.iterrows()):
                self.assertIn(exp_key, res_schema)
                res_key = exp_key
                res_it = res_schema[res_key]
                self.assertIsInstance(res_it, dict)
                self.assertEqual(exp_it['backend_db_field_name'], res_it[self.csv_to_json_col_map['backend_db_field_name']])
                self.assertEqual(exp_it['backend_table'], res_it[self.csv_to_json_col_map['backend_table']])
                self.assertEqual(exp_it['blank'], self.json_to_csv_blank_col_map[res_it[self.csv_to_json_col_map['blank']]])
                if res_it['py_dtype']:
                    self.assertEqual(
                        getattr(builtins, res_it['py_dtype'])(exp_it['default'] if res_it['py_dtype'] != 'bool' else int(exp_it['default'])) if exp_it['default'] is not None else None,
                        res_it[self.csv_to_json_col_map['default']]
                    )
                self.assertEqual(exp_it['desc'], res_it[self.csv_to_json_col_map['desc']])
                self.assertEqual(exp_it['entity'], res_it[self.csv_to_json_col_map['entity']])
                self.assertEqual(exp_it['field_name'], res_it[self.csv_to_json_col_map['field_name']])
                self.assertEqual(exp_it['required'], res_it[self.csv_to_json_col_map['required']])
                self.assertEqual(
                    exp_it['type'],
                    (
                        self.json_to_csv_type_col_map[res_it['sql_dtype']]
                        if res_it['sql_dtype'].endswith('int') or res_it['sql_dtype'].endswith('bit')
                        else res_it['sql_dtype']
                    )
                )

    def test_generate_schema__reinsscope(self):
        reinsscope_df = pd.read_csv(self.reinsscope_csv_schema_fp)
        reinsscope_df.columns = reinsscope_df.columns.str.lower()
        reinsscope_df = reinsscope_df.where(reinsscope_df.notnull(), None)
        reinsscope_df = reinsscope_df.sort_values(['entity', 'field_name']).reset_index(drop=True)

        with NamedTemporaryFile('w') as target_json:
            generate_schema(self.reinsscope_csv_schema_fp, target_json.name)
            with io.open(os.path.join('oedtools', 'schema', 'reinsscope_schema.json'), 'r', encoding='utf-8') as f: 
                res_schema = {literal_eval(k): v for k, v in json.load(f).items()}

            for exp_key, exp_it in (((it['entity'].lower(), it['field_name'].lower()), it) for _, it in reinsscope_df.iterrows()):
                self.assertIn(exp_key, res_schema)
                res_key = exp_key
                res_it = res_schema[res_key]
                self.assertIsInstance(res_it, dict)
                self.assertEqual(exp_it['backend_db_field_name'], res_it[self.csv_to_json_col_map['backend_db_field_name']])
                self.assertEqual(exp_it['backend_table'], res_it[self.csv_to_json_col_map['backend_table']])
                self.assertEqual(exp_it['blank'], self.json_to_csv_blank_col_map[res_it[self.csv_to_json_col_map['blank']]])
                if res_it['py_dtype']:
                    self.assertEqual(
                        getattr(builtins, res_it['py_dtype'])(exp_it['default'] if res_it['py_dtype'] != 'bool' else int(exp_it['default'])) if exp_it['default'] is not None else None,
                        res_it[self.csv_to_json_col_map['default']]
                    )
                self.assertEqual(exp_it['desc'], res_it[self.csv_to_json_col_map['desc']])
                self.assertEqual(exp_it['entity'], res_it[self.csv_to_json_col_map['entity']])
                self.assertEqual(exp_it['field_name'], res_it[self.csv_to_json_col_map['field_name']])
                self.assertEqual(exp_it['required'], res_it[self.csv_to_json_col_map['required']])
                self.assertEqual(
                    exp_it['type'],
                    (
                        self.json_to_csv_type_col_map[res_it['sql_dtype']]
                        if res_it['sql_dtype'].endswith('int') or res_it['sql_dtype'].endswith('bit')
                        else res_it['sql_dtype']
                    )
                )

    def test_get_schema__master(self):
        with io.open(self.master_json_schema_fp, 'r', encoding='utf-8') as f:
            exp_schema = OrderedDict({
                literal_eval(k): (
                    v if not v['dtype_range']
                    else {_k: (
                        _v if not _k == 'dtype_range'
                        else (
                            range(_v['start'], _v['stop']) if v['py_dtype'] == 'int' and isinstance(_v, dict) and 'start' in _v and 'stop' in _v
                            else _v
                        )
                    ) for _k, _v in v.items()
                    }
                )
                for k, v in json.load(f).items()
            })
        exp_schema = OrderedDict({
            k: (
                v if not isinstance(v.get('column_validation'), dict) or 'start' not in v['column_validation']
                else {**v, **{'column_validation': v['dtype_range']}}
            )
            for k, v in exp_schema.items()
        })
        
        res_schema = get_schema()

        self.assertEqual(exp_schema, res_schema)

    def test_get_schema__loc(self):
        with io.open(self.loc_json_schema_fp, 'r', encoding='utf-8') as f:
            exp_schema = OrderedDict({
                literal_eval(k): (
                    v if not v['dtype_range']
                    else {_k: (
                        _v if not _k == 'dtype_range'
                        else (
                            range(_v['start'], _v['stop']) if v['py_dtype'] == 'int' and isinstance(_v, dict) and 'start' in _v and 'stop' in _v
                            else _v
                        )
                    ) for _k, _v in v.items()
                    }
                )
                for k, v in json.load(f).items()
            })
        exp_schema = OrderedDict({
            k: (
                v if not isinstance(v.get('column_validation'), dict) or 'start' not in v['column_validation']
                else {**v, **{'column_validation': v['dtype_range']}}
            )
            for k, v in exp_schema.items()
        })
        
        res_schema = get_schema('loc')

        self.assertEqual(exp_schema, res_schema)

    def test_get_schema__acc(self):
        with io.open(self.acc_json_schema_fp, 'r', encoding='utf-8') as f:
            exp_schema = OrderedDict({
                literal_eval(k): (
                    v if not v['dtype_range']
                    else {_k: (
                        _v if not _k == 'dtype_range'
                        else (
                            range(_v['start'], _v['stop']) if v['py_dtype'] == 'int' and isinstance(_v, dict) and 'start' in _v and 'stop' in _v
                            else _v
                        )
                    ) for _k, _v in v.items()
                    }
                )
                for k, v in json.load(f).items()
            })
        exp_schema = OrderedDict({
            k: (
                v if not isinstance(v.get('column_validation'), dict) or 'start' not in v['column_validation']
                else {**v, **{'column_validation': v['dtype_range']}}
            )
            for k, v in exp_schema.items()
        })
        
        res_schema = get_schema('acc')

        self.assertEqual(exp_schema, res_schema)

    def test_get_schema__reinsinfo(self):
        with io.open(self.reinsinfo_json_schema_fp, 'r', encoding='utf-8') as f:
            exp_schema = OrderedDict({
                literal_eval(k): (
                    v if not v['dtype_range']
                    else {_k: (
                        _v if not _k == 'dtype_range'
                        else (
                            range(_v['start'], _v['stop']) if v['py_dtype'] == 'int' and isinstance(_v, dict) and 'start' in _v and 'stop' in _v
                            else _v
                        )
                    ) for _k, _v in v.items()
                    }
                )
                for k, v in json.load(f).items()
            })
        exp_schema = OrderedDict({
            k: (
                v if not isinstance(v.get('column_validation'), dict) or 'start' not in v['column_validation']
                else {**v, **{'column_validation': v['dtype_range']}}
            )
            for k, v in exp_schema.items()
        })
        
        res_schema = get_schema('reinsinfo')

        self.assertEqual(exp_schema, res_schema)

    def test_get_schema__reinsscope(self):
        with io.open(self.reinsscope_json_schema_fp, 'r', encoding='utf-8') as f:
            exp_schema = OrderedDict({
                literal_eval(k): (
                    v if not v['dtype_range']
                    else {_k: (
                        _v if not _k == 'dtype_range'
                        else (
                            range(_v['start'], _v['stop']) if v['py_dtype'] == 'int' and isinstance(_v, dict) and 'start' in _v and 'stop' in _v
                            else _v
                        )
                    ) for _k, _v in v.items()
                    }
                )
                for k, v in json.load(f).items()
            })
        exp_schema = OrderedDict({
            k: (
                v if not isinstance(v.get('column_validation'), dict) or 'start' not in v['column_validation']
                else {**v, **{'column_validation': v['dtype_range']}}
            )
            for k, v in exp_schema.items()
        })
        
        res_schema = get_schema('reinsscope')

        self.assertEqual(exp_schema, res_schema)

    def test_get_grouped_master_schema(self):
        with io.open(self.master_json_schema_fp, 'r', encoding='utf-8') as f:
            exp_master_schema = OrderedDict({
                literal_eval(k): (
                    v if not v['dtype_range']
                    else {_k: (
                        _v if not _k == 'dtype_range'
                        else (
                            range(_v['start'], _v['stop']) if v['py_dtype'] == 'int' and isinstance(_v, dict) and 'start' in _v and 'stop' in _v
                            else _v
                        )
                    ) for _k, _v in v.items()
                    }
                )
                for k, v in json.load(f).items()
            })
        exp_master_schema = OrderedDict({
            k: (
                v if not isinstance(v.get('column_validation'), dict) or 'start' not in v['column_validation']
                else {**v, **{'column_validation': v['dtype_range']}}
            )
            for k, v in exp_master_schema.items()
        })
        exp_grouped_master_schema = { 
            schema_type: { 
                item_key[1]: item 
                for item_key, item in schema_items 
            } 
            for schema_type, schema_items in groupby(exp_master_schema.items(), key=lambda it: it[0][0]) 
        }

        res_grouped_master_schema = get_grouped_master_schema()
        self.assertEqual(exp_grouped_master_schema, res_grouped_master_schema)

    def test_get_schema_version(self):
        with io.open(self.schema_version_fp, 'r', encoding='utf-8') as f:
            exp_version = f.readlines()[0].strip()

        res_version = get_schema_version()
        self.assertEqual(exp_version, res_version)

    @given(
        column=text(alphabet=string.ascii_lowercase, min_size=1, max_size=50)
    )
    def test_get_column_schema__master_schema_type__raises_oed_error(self, column):
        with self.assertRaises(OedError):
            get_column_schema('master', column)

    @given(
        schema_type=sampled_from(SCHEMA_TYPES_EX_MASTER),
        column=sampled_from([t[1] for t in ALL]),
        random_str=text(alphabet=string.ascii_lowercase, min_size=1)
    )
    def test_get_column_schema__invalid_schema_type_and_column__raises_non_oed_schema_and_column_error(self, schema_type, column, random_str):
        with self.assertRaises(NonOedSchemaAndColumnError):
            get_column_schema(schema_type + random_str, column + random_str)

    @given(
        schema_type=sampled_from(SCHEMA_TYPES_EX_MASTER),
        column=sampled_from([t[1] for t in ALL]),
        random_str=text(alphabet=string.ascii_lowercase, min_size=1)
    )
    def test_get_column_schema__invalid_schema_type_but_valid_column__raises_non_oed_schema_error(self, schema_type, column, random_str):
        with self.assertRaises(NonOedSchemaError):
            get_column_schema(schema_type + random_str, column)

    @given(
        schema_type=sampled_from(SCHEMA_TYPES_EX_MASTER)
    )
    def test_get_column_schema__valid_schema_type_but_invalid_schema_column__raises_non_oed_schema_column_error(self, schema_type):
        column = np.random.choice([
            col for stype, col_dict in get_grouped_master_schema().items()
            for col in col_dict
            if stype != schema_type
        ])
        with self.assertRaises(NonOedSchemaColumnError):
            get_column_schema(schema_type, column)

    @given(
        schema_type=sampled_from(SCHEMA_TYPES_EX_MASTER),
        column=sampled_from([t[1] for t in ALL]),
        random_str=text(alphabet=string.ascii_lowercase, min_size=1)
    )
    def test_get_column_schema__valid_schema_type_but_invalid_column__raises_non_oed_column_error(self, schema_type, column, random_str):
        with self.assertRaises(NonOedColumnError):
            get_column_schema(schema_type, column + random_str)

    @given(
        column=sampled_from(ACC)
    )
    def test_get_column_schema__valid_acc_column(self, column):
        exp_col_schema = self.acc_schema[('acc', column.lower())]
        res_col_schema = get_column_schema('acc', column.lower())
        self.assertEqual(exp_col_schema, res_col_schema)

    @given(
        column=sampled_from(LOC)
    )
    def test_get_column_schema__valid_loc_column(self, column):
        exp_col_schema = self.loc_schema[('loc', column.lower())]
        res_col_schema = get_column_schema('loc', column.lower())
        self.assertEqual(exp_col_schema, res_col_schema)

    @given(
        column=sampled_from(REINSINFO)
    )
    def test_get_column_schema__valid_reinsinfo_column(self, column):
        exp_col_schema = self.reinsinfo_schema[('reinsinfo', column.lower())]
        res_col_schema = get_column_schema('reinsinfo', column.lower())
        self.assertEqual(exp_col_schema, res_col_schema)

    @given(
        column=sampled_from(REINSSCOPE)
    )
    def test_get_column_schema__valid_reinsscope_column(self, column):
        exp_col_schema = self.reinsscope_schema[('reinsscope', column.lower())]
        res_col_schema = get_column_schema('reinsscope', column.lower())
        self.assertEqual(exp_col_schema, res_col_schema)

    def test_update_schemas(self):
        values_profile_last_modified = datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'values.json')).st_mtime)
        master_schema_last_modified = datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'master_schema.json')).st_mtime)
        loc_schema_last_modified = datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'loc_schema.json')).st_mtime)
        acc_schema_last_modified = datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'acc_schema.json')).st_mtime)
        reinsinfo_schema_last_modified = datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'reinsinfo_schema.json')).st_mtime)
        reinsscope_schema_last_modified = datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'reinsscope_schema.json')).st_mtime)

        update_schemas()

        self.assertTrue(
            datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'values.json')).st_mtime) >
            values_profile_last_modified
        )
        self.assertTrue(
            datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'master_schema.json')).st_mtime) >
            master_schema_last_modified
        )
        self.assertTrue(
            datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'loc_schema.json')).st_mtime) >
            loc_schema_last_modified
        )
        self.assertTrue(
            datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'acc_schema.json')).st_mtime) >
            acc_schema_last_modified
        )
        self.assertTrue(
            datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'reinsinfo_schema.json')).st_mtime) >
            reinsinfo_schema_last_modified
        )
        self.assertTrue(
            datetime.fromtimestamp(os.stat(os.path.join(SCHEMA_DIR, 'reinsscope_schema.json')).st_mtime) >
            reinsscope_schema_last_modified
        )

    @given(
        schema_key=sampled_from(ALL)
    )
    def test_sample_column(self, schema_key):
        schema_type, header = schema_key
        col_schema = self.master_schema[(schema_type, header)]
        exp_py_dtype, use_range = getattr(builtins, col_schema['py_dtype']), col_schema['column_range'] or col_schema['dtype_range']
        validation_src = col_schema['column_validation']
        validation_func = (
            get_method(validation_src.replace('func:', '')) if isinstance(validation_src, str) and validation_src.startswith('func:')
            else None
        )
        try:
            sampling_info = json.loads(col_schema['column_sampling'])
        except (JSONDecodeError, TypeError, ValueError):
            sampling_info = sampling_func = None
        else:
            sampling_func = get_method(sampling_info['func'])

        sample = sample_column(schema_type, header)

        if exp_py_dtype in [int, float] and validation_func is not None:
            self.assertTrue(all(validation_func(use_range, value) for value in sample))
        elif exp_py_dtype is int:
            self.assertTrue(all(value in use_range for value in sample))
        elif exp_py_dtype is float:
            self.assertTrue(all(value >= min(use_range) and value <= max(use_range) for value in sample))
        if exp_py_dtype is str and use_range is not None and validation_func is not None:
            self.assertTrue(all(validation_func(use_range, value) for value in sample))
        elif exp_py_dtype is str and use_range is not None:
            self.assertTrue(all(value in use_range for value in sample))
        elif exp_py_dtype is str:
            self.assertTrue(all(isinstance(value, str) for value in sample))
