__all__ = [
    'generate_schema',
    'get_column_schema',
    'get_grouped_master_schema',
    'get_schema',
    'get_schema_version',
    'sample_column',
    'SCHEMA_DIR',
    'update_schemas'
]


"""
Package utilities
"""

import builtins
import io
import json
import string
import os

from ast import literal_eval
from collections import OrderedDict
from itertools import groupby

import pandas as pd
import numpy as np

from .exceptions import (
    get_file_error,
    OedError,
)
from .utils import (
    get_method,
    SQL_NUMERIC_DTYPES,
    sql_to_python_dtype,
)
from .values import (
    generate_values_profile,
    get_column_range_by_value_group,
    get_column_sampling_method,
    get_column_validation_method,
    get_values_profile,
)


SCHEMA_DIR = os.path.join(os.path.dirname(__file__), 'schema')


def generate_schema(def_fp, target_fp):
    """
    Generates a JSON schema from a CSV schema for a given OED file type (acc.
    loc., reins. info., or reins. scope), including the master file schema,
    and writes it to target file.

    :param def_fp: Path of the CSV schema (data definition) file for a type of
                   OED input file, e.g. loc., acc., reins. info. or reins. scope
    :type def_fp: str

    :param target_fp: The target file path to write the schema to
    :type target_fp: str
    """
    _def_fp = os.path.abspath(def_fp)
    _target_fp = os.path.abspath(target_fp) if target_fp else None

    def_df = pd.read_csv(_def_fp, memory_map=True).drop_duplicates()
    def_df.columns = def_df.columns.str.lower()
    def_df = def_df.where(def_df.notnull(), None)

    def_df.rename(
        columns={
            'backend_table': 'oed_db_table',
            'backend_db_field_name': 'oed_db_field_name',
            'type': 'sql_dtype'
        },
        inplace=True
    )

    def_df['entity'] = def_df['entity'].apply(lambda s: s.replace(' ', ''))
    def_df = def_df.sort_values(['entity', 'field_name']).reset_index(drop=True)

    def_df['sql_dtype'] = def_df['sql_dtype'].fillna('char')
    def_df['sql_dtype'] = def_df['sql_dtype'].str.lower().replace({'0 or 1': 'bit'})

    all_bool_and_int_cols = sorted(
        def_df[
            def_df['sql_dtype'].str.lower().isin(['bit', 'tinyint', 'smallint', 'int', 'bigint'])
        ]['field_name'].tolist()
    )
    bool_and_int_col_dtypes = def_df[
        def_df['field_name'].str.lower().isin([c.lower() for c in all_bool_and_int_cols])
    ]['sql_dtype']
    bool_and_int_col_dtypes = 'unsigned ' + bool_and_int_col_dtypes
    def_df.loc[bool_and_int_col_dtypes.index, ['sql_dtype']] = bool_and_int_col_dtypes

    def_df['py_dtype'] = def_df['sql_dtype'].apply(sql_to_python_dtype)
    def_df['numpy_dtype'] = def_df['sql_dtype'].apply(sql_to_python_dtype, as_numpy_dtype=True)

    sql_numeric_dtype_ranges = {k: v['range'] for k, v in SQL_NUMERIC_DTYPES.items()}
    def_df['dtype_range'] = def_df['sql_dtype'].apply(lambda dt: sql_numeric_dtype_ranges.get(dt))

    values_profile = get_values_profile()

    def_df['column_range'] = def_df['field_name'].apply(get_column_range_by_value_group, values_profile=values_profile)

    def_df['column_sampling'] = def_df['field_name'].apply(get_column_sampling_method, values_profile=values_profile)

    def_df['column_validation'] = def_df['field_name'].apply(get_column_validation_method, values_profile=values_profile)
    def_df['column_validation'] = np.where(def_df['column_validation'].notnull(), def_df['column_validation'], def_df['dtype_range'])

    def_df['blank'] = def_df['blank'].str.lower().replace({'yes': True, 'no': False})
    def_df['default'] = def_df.loc[:, ['py_dtype', 'default']].apply(
        lambda it: (
            int(float(it['default'])) if it['default'] is not None and it['py_dtype'] in ['int', 'bool']
            else (float(it['default']) if it['default'] is not None and it['py_dtype'] == 'float' else it['default'])
        ), axis=1
    )

    schema = OrderedDict({
        str((it['entity'].lower().strip(' '), it['field_name'].lower())): {
            k: (
                v if k not in ['default', 'secmod'] else (getattr(builtins, it['py_dtype'])(v) if it['py_dtype'] is not None and v is not None else None)
            ) for k, v in it.items()
        }
        for it in json.loads(def_df.sort_values(by=['entity', 'field_name']).to_json(orient='records'))
    })

    with io.open(os.path.abspath(_target_fp), 'w', encoding='utf-8') as f:
        f.write(json.dumps(schema, indent=4, sort_keys=True))
        f.flush()


def get_schema(schema_type='master'):
    """
    Gets the schema of an OED input file - schema type must be one of
    ``acc``, ``loc``, ``reinsinfo``, ``reinsscope``.

    :param schema_type: (Optional) File schema type indicator (``master``,
                        ``loc``, ``acc``, ``reinsinfo``, or ``reinsscope``)
    :type schema_type: str

    :return: The schema dict
    :rtype: dict
    """
    with io.open(os.path.join(SCHEMA_DIR, '{}_schema.json'.format(schema_type.lower()))) as f:
        schema = OrderedDict({
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
    return OrderedDict({
        k: (
            v if not isinstance(v.get('column_validation'), dict) or ('start' not in v['column_validation'] and 'stop' not in v['column_validation'])
            else {**v, **{'column_validation': v['dtype_range']}}
        )
        for k, v in schema.items()
    })


def get_grouped_master_schema():
    """
    Gets the master schema grouped by schema type.

    :return: Master schema grouped by schema type
    :rtype: dict
    """
    return {
        schema_type: {
            item_key[1]: item
            for item_key, item in schema_items
        }
        for schema_type, schema_items in groupby(get_schema().items(), key=lambda it: it[0][0])
    }


def get_schema_version():
    """
    Gets the OED schema version - the schema version is stored in the
    ``schema_version.txt`` file in the ``schema`` subfolder. The version
    will correspond to the current Simplitium OED release version (1.0.3,
    as of 22/06/2019).

    :return: OED schema version
    :rtype: str
    """
    with io.open(os.path.join(SCHEMA_DIR, 'schema_version.txt')) as f:
        return f.readlines()[0].strip()


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
    except (json.JSONDecodeError, TypeError, ValueError):
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
