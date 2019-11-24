__all__ = [
    'generate_values_profile',
    'get_column_range_by_value_group',
    'get_column_sampling_method',
    'get_column_validation_method',
    'get_values_profile',
    'SCHEMA_DIR'
]

import io
import json
import os
import re

from ast import literal_eval
from collections import OrderedDict
from itertools import groupby

import pandas as pd


SCHEMA_DIR = os.path.join(os.path.dirname(__file__), 'schema')


def generate_values_profile(target_fp=None):
    """
    Generates a JSON profile of values across key value groups described in the
    OED spec. such as construction codes/class, occupancy codes/types, peril
    codes, coverage types, deductible types and codes, limit types and codes,
    etc., as expressed in the input file columns.

    This can be used to understand the structure of any value groups of
    interest, and also to validate actual values in the columns associated with
    any of these value groups, e.g. checking that the location level deductible
    code column for the buildings coverage type (`LocDedCode1Building`) only
    takes values in the set {1,2,3,4,5,6}.

    :param target_fp: (Optional) The target file path to write the profile to
    :type target_fp: str

    :return: A values profile dict if no target file path
    :rtype: dict
    """
    _target_fp = os.path.abspath(target_fp) if target_fp else None

    def_df = pd.read_csv(os.path.join(SCHEMA_DIR, 'master_def.csv'))
    def_df.columns = def_df.columns.str.lower()
    all_cols = sorted(def_df['field_name'].unique().tolist())

    values_csv_profile = pd.read_csv(os.path.join(SCHEMA_DIR, 'values.csv'))
    values_csv_profile['id'] = values_csv_profile['id'].fillna('')

    values_csv_profile.sort_values(by=['group', 'key', 'id'], inplace=True)

    items = values_csv_profile.to_dict(orient='records')

    def regex_match(match_str, astring):
        return re.match(r'{}'.format(match_str), astring)

    values_profile = OrderedDict({
        val_group_name: OrderedDict({
            it['key']: {
                'id': it['id'],
                'desc': it['desc'],
                'columns': sorted([col for col in all_cols if regex_match(it['column_name_regex'], col.lower())]),
                'sampling': it['sampling'],
                'validation': it['validation']
            } for it in val_group
        }) for val_group_name, val_group in groupby(items, key=lambda it: it['group'])
    })

    if not _target_fp:
        return values_profile

    with io.open(os.path.abspath(_target_fp), 'w', encoding='utf-8') as f:
        f.write(json.dumps(values_profile, indent=4, sort_keys=True))
        f.flush()

    return _target_fp


def get_values_profile():
    """
    Gets the values profile JSON (from ``oedtools/schema/``) as a dict.

    :return: The values profile dict
    :rtype: dict
    """
    with io.open(os.path.join(SCHEMA_DIR, 'values.json'), 'r', encoding='utf-8') as f:
        return json.load(f)


def get_column_range_by_value_group(header, values_profile=get_values_profile()):
    """
    Gets the range of values of a given column from an OED input file (if
    present in the current master schema), using the information in the values
    profile.

    :param header: Column header (case insensitive)
    :type header: str

    :param values_profile: (Optional) Values profile - this can save time for
                           a caller applying this method on multiple columns
    :type values_profile: dict

    :return: The column values range as a list if not null, or `None`
    :rtype: list, None
    """
    subval_strs = set([
        v['id']
        for group_dict in values_profile.values()
        for v in group_dict.values()
        for _header in v['columns']
        if _header.lower() == header.lower()
    ])

    def subval_str_to_list(subval_str):
        q = re.match(r'(-?\d+):(-?\d+)$', subval_str)
        if q:
            return list(range(int(q.groups()[0]), int(q.groups()[1]) + 1))
        q = re.match(r'(-|\+){0,1}?(\d+)?(\.)?(\d+)?(e\+\d+|e-\d+)?:(-|\+){0,1}?(\d+)?(\.)?(\d+)?(e\+\d+|e-\d+)?$', subval_str)
        if q:
            lb = literal_eval(''.join([s for s in q.groups()[:5] if s is not None]))
            ub = literal_eval(''.join([s for s in q.groups()[5:] if s is not None]))
            return (min(lb, ub), max(lb, ub))
        try:
            return [int(subval_str)]
        except (TypeError, ValueError):
            return [subval_str]

    val_range = set([v for s in subval_strs for v in subval_str_to_list(s) if v not in [None, '']])
    try:
        val_range = sorted(val_range)
    except TypeError:
        val_range = list(val_range)

    return val_range or None


def get_column_sampling_method(header, values_profile=get_values_profile()):
    """
    Indicates how to sample the values in a given column, according to
    the values profile - the standard method is to use the column range as
    inferred from values profile, but any value-generation method in the
    package can be used, as long as it is specified as a lowercase string
    prefixed with ``func:`` and terminates with the full package path of the
    method.

    :param header: The column header (case insensitive)
    :type header: str

    :param values_profile: (Optional) Values profile
    :type values_profile: dict

    :return: The column sampling method
    :rtype: str
    """
    try:
        return [
            v['sampling']
            for group_dict in values_profile.values()
            for v in group_dict.values()
            if header.lower() in [_header.lower() for _header in v['columns']]
        ][0]
    except IndexError:
        return


def get_column_validation_method(header, values_profile=get_values_profile()):
    """
    Indicates how  to validate the values in a given column, according to the
    values profile - the standard method is to use the column range as inferred
    from values profile, but any validation method in the package can be used,
    as long as it is specified as a lowercase string prefixed with ``func:``
    and terminates with the full package path of the method.

    :param header: The column header (case insensitive)
    :type header: str

    :param values_profile: (Optional) Values profile
    :type values_profile: dict

    :return: The column validation method
    :rtype: str
    """
    try:
        return [
            v['validation']
            for group_dict in values_profile.values()
            for v in group_dict.values()
            if header.lower() in [_header.lower() for _header in v['columns']]
        ][0]
    except IndexError:
        return
