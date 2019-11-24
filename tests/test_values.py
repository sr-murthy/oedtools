import io
import json
import os
import re
import time

from ast import literal_eval
from collections import OrderedDict
from datetime import datetime
from tempfile import NamedTemporaryFile
from unittest import TestCase

import pandas as pd
import pytest

from hypothesis import (
    given,
    settings,
)
from hypothesis.strategies import (
    text,
)

from oedtools.values import (
    generate_values_profile,
    get_column_range_by_value_group,
    get_column_sampling_method,
    get_column_validation_method,
    get_values_profile,
    SCHEMA_DIR,
)


class TestValues(TestCase):

    def setUp(self):
        self.SCHEMA_DIR = SCHEMA_DIR
        self.values_csv_fp = os.path.join(self.SCHEMA_DIR, 'values.csv')
        self.master_csv_schema_fp = os.path.join(self.SCHEMA_DIR, 'master_def.csv')
        self.loc_csv_schema_fp = os.path.join(self.SCHEMA_DIR, 'loc_def.csv')
        self.acc_csv_schema_fp = os.path.join(self.SCHEMA_DIR, 'acc_def.csv')
        self.reinsinfo_csv_schema_fp = os.path.join(self.SCHEMA_DIR, 'reinsinfo_def.csv')
        self.reinsscope_csv_schema_fp = os.path.join(self.SCHEMA_DIR, 'reinsscope_def.csv')

    def test_generate_values_profile__write_to_target_file(self):
        values_df = pd.read_csv(self.values_csv_fp)
        values_df.columns = values_df.columns.str.lower()

        master_df = pd.read_csv(self.master_csv_schema_fp)
        master_df.columns = master_df.columns.str.lower()

        with NamedTemporaryFile('w') as target_file:
            target_fp = generate_values_profile(target_file.name)
            self.assertEqual(target_fp, target_file.name)

            with io.open(target_file.name, 'r', encoding='utf-8') as f:
                values_profile = json.load(f)

            groups = list(values_profile.keys())
            self.assertEqual(groups, sorted(values_df['group'].unique().tolist()))
            for g in groups:
                keys = list(values_profile[g].keys())
                group_df = values_df[values_df['group'] == g]
                group_df = group_df.fillna('')
                self.assertEqual(keys, sorted(group_df['key'].tolist()))
                for k in keys:
                    it = group_df[group_df['key'] == k].iloc[0]
                    self.assertEqual(values_profile[g][k]['id'], it['id'])
                    self.assertEqual(values_profile[g][k]['desc'], it['desc'])
                    it_columns = sorted(master_df[
                        master_df['field_name'].str.lower().str.match(r'{}'.format(it['column_name_regex']))
                    ]['field_name'].unique().tolist()) if it['column_name_regex'] else []
                    self.assertEqual(values_profile[g][k]['columns'], it_columns)
                    self.assertEqual(values_profile[g][k]['sampling'], it['sampling'])
                    self.assertEqual(values_profile[g][k]['validation'], it['validation'])

    def test_generate_values_profile__no_target_file__return_as_dict(self):
        values_df = pd.read_csv(self.values_csv_fp)
        values_df.columns = values_df.columns.str.lower()

        master_df = pd.read_csv(self.master_csv_schema_fp)
        master_df.columns = master_df.columns.str.lower()

        values_profile = generate_values_profile()
        self.assertIsInstance(values_profile, dict)

        groups = list(values_profile.keys())
        self.assertEqual(groups, sorted(values_df['group'].unique().tolist()))
        for g in groups:
            keys = list(values_profile[g].keys())
            group_df = values_df[values_df['group'] == g]
            group_df = group_df.fillna('')
            self.assertEqual(keys, sorted(group_df['key'].tolist()))
            for k in keys:
                it = group_df[group_df['key'] == k].iloc[0]
                self.assertEqual(values_profile[g][k]['id'], it['id'])
                self.assertEqual(values_profile[g][k]['desc'], it['desc'])
                it_columns = sorted(master_df[
                    master_df['field_name'].str.lower().str.match(r'{}'.format(it['column_name_regex']))
                ]['field_name'].unique().tolist()) if it['column_name_regex'] else []
                self.assertEqual(values_profile[g][k]['columns'], it_columns)
                self.assertEqual(values_profile[g][k]['sampling'], it['sampling'])
                self.assertEqual(values_profile[g][k]['validation'], it['validation'])

    def test_get_values_profile(self):
        values_df = pd.read_csv(self.values_csv_fp)
        values_df.columns = values_df.columns.str.lower()

        master_df = pd.read_csv(self.master_csv_schema_fp)
        master_df.columns = master_df.columns.str.lower()

        values_profile = get_values_profile()
        self.assertIsInstance(values_profile, dict)

        groups = list(values_profile.keys())
        self.assertEqual(groups, sorted(values_df['group'].unique().tolist()))
        for g in groups:
            keys = list(values_profile[g].keys())
            group_df = values_df[values_df['group'] == g]
            group_df = group_df.fillna('')
            self.assertEqual(keys, sorted(group_df['key'].tolist()))
            for k in keys:
                it = group_df[group_df['key'] == k].iloc[0]
                self.assertEqual(values_profile[g][k]['id'], it['id'])
                self.assertEqual(values_profile[g][k]['desc'], it['desc'])
                it_columns = sorted(master_df[
                    master_df['field_name'].str.lower().str.match(r'{}'.format(it['column_name_regex']))
                ]['field_name'].unique().tolist()) if it['column_name_regex'] else []
                self.assertEqual(values_profile[g][k]['columns'], it_columns)
                self.assertEqual(values_profile[g][k]['sampling'], it['sampling'])
                self.assertEqual(values_profile[g][k]['validation'], it['validation'])

    def test_get_column_range_by_value_group(self):
        values_df = pd.read_csv(self.values_csv_fp)
        values_df.columns = values_df.columns.str.lower()

        master_df = pd.read_csv(self.master_csv_schema_fp)
        master_df.columns = master_df.columns.str.lower()

        values_profile_cols = []

        area_codes = sorted(set(values_df[values_df['group'] == 'area codes']['id'].sort_values().tolist()))
        def to_int(s):
            try:
                return int(s)
            except ValueError:
                return s
        area_codes = [to_int(c) for c in area_codes]
        self.assertEqual(set(area_codes), set(get_column_range_by_value_group('AreaCode')))
        values_profile_cols += ['AreaCode']

        construction_codes = sorted(set([
            _v for v in [
                range(int(s.split(':')[0]), int(s.split(':')[1]) + 1) if re.match(r'(\d+):(\d+)$', s)
                else ([int(s)] if re.match(r'\d+$', s) else None)
                for s in [
                    it['id'] for _, it in values_df[values_df['group'] == 'construction codes'].iterrows()
                ]
            ] for _v in v if _v
        ]))
        self.assertEqual(construction_codes, get_column_range_by_value_group('ConstructionCode'))
        values_profile_cols = ['ConstructionCode']

        country_codes = sorted(set(values_df[values_df['group'] == 'country codes']['id'].sort_values().tolist()))
        self.assertEqual(country_codes, get_column_range_by_value_group('CountryCode'))
        values_profile_cols += ['CountryCode']

        currencies = sorted(set([v for v in values_df[values_df['group'] == 'currencies']['id'].sort_values().fillna('').tolist() if v]))
        cols = master_df[master_df['field_name'].str.lower().str.match(r'(acc|loc|reins)currency?')]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(currencies, get_column_range_by_value_group(col))
        values_profile_cols += cols

        is_primary = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'location properties') & (values_df['key'] == 'is primary')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(is_primary, get_column_range_by_value_group('IsPrimary'))
        values_profile_cols += ['IsPrimary']

        is_tenant = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'location properties') & (values_df['key'] == 'is tenant')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(is_primary, get_column_range_by_value_group('IsTenant'))
        values_profile_cols += ['IsTenant']

        tivs = [
            literal_eval(s) for s in
            values_df[values_df['group'] == 'tivs'].iloc[0]['id'].split(':')
        ]
        cols = master_df[
            master_df['field_name'].str.lower().str.match(r'(building|contents|other|bi)tiv$')
        ]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(tivs, get_column_range_by_value_group(col))
        values_profile_cols += cols

        deductibles = [
            literal_eval(s) for s in
            values_df[values_df['group'] == 'deductibles'].iloc[0]['id'].split(':')
        ]
        cols = master_df[
            master_df['field_name'].str.lower().str.match(r'(acc|cond|loc|pol)(min|max)?ded([1-6])(building|other|contents|bi|pd|all)$')
        ]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(deductibles, get_column_range_by_value_group(col))
        values_profile_cols += cols

        deductible_codes = sorted(set([
            int(c) for c in values_df[values_df['group']=='deductible codes']['id'].sort_values().tolist()
        ]))
        cols = master_df[master_df['field_name'].str.lower().str.contains('dedcode')]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(deductible_codes, get_column_range_by_value_group(col))
        values_profile_cols += cols

        deductible_types = sorted(set([
            int(c) for c in values_df[values_df['group']=='deductible types']['id'].sort_values().tolist()
        ]))
        cols = master_df[master_df['field_name'].str.lower().str.contains('dedtype')]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(deductible_types, get_column_range_by_value_group(col))
        values_profile_cols += cols

        sublayer_limits = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'limits') & (values_df['key'] == 'sublayer')].iloc[0]['id'].split(':')
        ]
        cols = master_df[
            master_df['field_name'].str.lower().str.match(r'(acc|cond|loc|pol)limit([1-6])(building|other|contents|bi|pd|all)$')
        ]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(sublayer_limits, get_column_range_by_value_group(col))
        values_profile_cols += cols

        layer_attachment = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'attachments') & (values_df['key'] == 'layer')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(layer_attachment, get_column_range_by_value_group('LayerAttachment'))
        values_profile_cols += ['LayerAttachment']

        layer_limit = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'limits') & (values_df['key'] == 'layer')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(layer_limit, get_column_range_by_value_group('LayerLimit'))
        values_profile_cols += ['LayerLimit']

        location_share = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'shares') & (values_df['key'] == 'location')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(location_share, get_column_range_by_value_group('LocParticipation'))
        values_profile_cols += ['LocParticipation']

        layer_share = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'shares') & (values_df['key'] == 'layer')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(layer_share, get_column_range_by_value_group('LayerParticipation'))
        values_profile_cols += ['LayerParticipation']

        limit_codes = sorted(set([
            int(c) for c in values_df[values_df['group']=='limit codes']['id'].sort_values().tolist()
        ]))
        cols = master_df[master_df['field_name'].str.lower().str.contains('limitcode')]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(limit_codes, get_column_range_by_value_group(col))
        values_profile_cols += cols

        limit_types = sorted(set([
            int(c) for c in values_df[values_df['group']=='limit types']['id'].sort_values().tolist()
        ]))
        cols = master_df[master_df['field_name'].str.lower().str.contains('limittype')]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(limit_types, get_column_range_by_value_group(col))
        values_profile_cols += cols

        longitude = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'geocoding') & (values_df['key'] == 'longitude')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(longitude, get_column_range_by_value_group('Longitude'))
        values_profile_cols += ['Longitude']

        latitude = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'geocoding') & (values_df['key'] == 'latitude')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(latitude, get_column_range_by_value_group('Latitude'))
        values_profile_cols += cols

        occupancy_types = sorted(set([
            _v for v in [
                range(int(s.split(':')[0]), int(s.split(':')[1]) + 1) if re.match(r'(\d+):(\d+)$', s)
                else ([int(s)] if re.match(r'\d+$', s) else None)
                for s in [it['id'] for _, it in values_df[values_df['group'] == 'occupancy types'].iterrows()]
            ] for _v in v if _v
        ]))
        self.assertEqual(occupancy_types, get_column_range_by_value_group('OccupancyCode'))
        values_profile_cols += ['OccupancyCode']

        peril_codes = sorted(set(values_df[values_df['group'] == 'peril codes']['id'].sort_values().tolist()))
        cols = master_df[master_df['field_name'].str.lower().str.match(r'(acc|loc|pol|reins)peril(scovered)?$')]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(peril_codes, get_column_range_by_value_group(col))
        values_profile_cols += cols

        years = sorted([
            _v for v in [
                range(int(s.split(':')[0]), int(s.split(':')[1]) + 1) if re.match(r'(\d+):(\d+)$', s)
                else ([int(s)] if re.match(r'\d+$', s) else None)
                for s in [it['id'] for _, it in values_df[values_df['group'] == 'years'].iterrows()]
            ] for _v in v if _v
        ])
        cols = master_df[master_df['field_name'].str.lower().str.match(r'(roofyearbuilt|yearbuilt|yearupgraded)$')]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(years, get_column_range_by_value_group(col))
        values_profile_cols += cols

        reins_types = sorted(set(values_df[values_df['group'] == 'reins types']['id'].sort_values().tolist()))
        cols = master_df[master_df['field_name'].str.lower().str.match(r'reinstype$')]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(reins_types, get_column_range_by_value_group(col))
        values_profile_cols += cols

        reins_placed_percent = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'reins percentages') & (values_df['key'] == 'placed')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(reins_placed_percent, get_column_range_by_value_group('PlacedPercent'))
        values_profile_cols += ['PlacedPercent']

        reins_ceded_percent = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'reins percentages') & (values_df['key'] == 'ceded')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(reins_ceded_percent, get_column_range_by_value_group('CededPercent'))
        values_profile_cols += ['CededPercent']

        reins_treaty_share = [
            literal_eval(s) for s in
            values_df[(values_df['group'] == 'reins percentages') & (values_df['key'] == 'treatyshare')].iloc[0]['id'].split(':')
        ]
        self.assertEqual(reins_treaty_share, get_column_range_by_value_group('TreatyShare'))
        values_profile_cols += ['TreatyShare']

        reins_risk_levels = sorted(set(values_df[values_df['group'] == 'reins risk levels']['id'].sort_values().tolist()))
        cols = master_df[master_df['field_name'].str.lower().str.match(r'risklevel$')]['field_name'].sort_values().tolist()
        for col in cols:
            self.assertEqual(reins_risk_levels, get_column_range_by_value_group(col))
        values_profile_cols += cols

    def test_get_column_sampling_method(self):
        values_df = pd.read_csv(self.values_csv_fp)
        values_df.columns = values_df.columns.str.lower()

        master_df = pd.read_csv(self.master_csv_schema_fp)
        master_df.columns = master_df.columns.str.lower()

        all_values_profile_cols = sorted([
            col for column_name_regex in values_df['column_name_regex'].unique().tolist()
            for col in master_df[master_df['field_name'].str.lower().str.match(r'{}'.format(column_name_regex))]['field_name'].tolist() 
        ])

        for col in master_df['field_name'].tolist():
            expected = values_df.loc[:, ['column_name_regex', 'sampling']].apply(
                lambda it: it['sampling'] if re.match(r'{}'.format(it['column_name_regex']), col.lower()) else None,
                axis=1
            ).dropna().unique().tolist() or None
            self.assertEqual(expected[0] if expected else None, get_column_sampling_method(col))

    def test_get_column_validation_method(self):
        values_df = pd.read_csv(self.values_csv_fp)
        values_df.columns = values_df.columns.str.lower()

        master_df = pd.read_csv(self.master_csv_schema_fp)
        master_df.columns = master_df.columns.str.lower()

        all_values_profile_cols = sorted([
            col for column_name_regex in values_df['column_name_regex'].unique().tolist()
            for col in master_df[master_df['field_name'].str.lower().str.match(r'{}'.format(column_name_regex))]['field_name'].tolist() 
        ])

        for col in master_df['field_name'].tolist():
            expected = values_df.loc[:, ['column_name_regex', 'validation']].apply(
                lambda it: it['validation'] if re.match(r'{}'.format(it['column_name_regex']), col.lower()) else None,
                axis=1
            ).dropna().unique().tolist() or None
            self.assertEqual(expected[0] if expected else None, get_column_validation_method(col))
