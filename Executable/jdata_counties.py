import re

import numpy as np
import pandas as pd

from hud_geo_conversions import read_zips_to_fips
from jdata import JData


class JDataCounties(JData):
    """Counts of JData orgs for US counties by type and denomination."""

    def __init__(self, jdata_org_fp, zip_to_fips_fp):
        """
        Reads

        Parameters
        ----------
        jdata_org_fp : str, filepath to jdata json file
        zip_to_fips_fp : str, filepath to HUD Zip-FIPS code
            conversion table (xlsx)

        Attributes
        ----------
        orgs_df  : DataFrame of individual organizations
        _zips_df : DataFrame, intermediate step of Zip counts
            before aggregating further to counties
        cnty_df  : DataFrame of organization county indexed to
            FIPS county codes used by US Census Bureau
        """
        self.zip_to_fips_fp = zip_to_fips_fp
        self.to_fips = read_zips_to_fips(zip_to_fips_fp)
        self.jdata_org_fp = jdata_org_fp

        # Allows the client to modify organization data before
        # reaggregating it
        self.orgs_df = (self.read_data(jdata_org_fp)
                        .pipe(self.clean_data)
                        .pipe(self.reduce_denoms))
        self._zips_df = None  # initialized in get_county_cnts()
        # County data initialized with default values, can be called
        # again with different parameters for analysis
        self.cnty_df = self.get_county_cnts()

    def _missing_zip_to_nearest(self, zips_df, to_fips, msg=False):
        """
        Replace zip codes in df that are not present in
        zip-to-county conversion table. Replacements are
        estimates based on nearest ordinal proximity of zip.

        Parameters
        ----------
        zips_df : pandas.DataFrame, must be indexed by zip code
        to_fips : pandas.DataFrame, must have index of ZIPS,
                  one-to-many zip-county conversion table
        msg : bool, if true, print old and new zips for analysis

        Returns
        ----------
        df : pandas.DataFrame, zip_cnts with missing zips fixed
        """
        missing = set(zips_df.index) - set(to_fips.index)
        zip_arr = to_fips.index.astype(int).values
        zip_ests = {}
        for zip_ in missing:
            idx = np.abs(zip_arr - int(zip_)).argmin()
            zip_est = str(zip_arr[idx]).zfill(5)
            zip_ests[zip_] = zip_est

        if msg:
            print('Zips not in HUD conversion table:')
            print(list(zip_ests.keys()))
            print('replaced with')
            print(list(zip_ests.values()))

        # merge imputed zips with existing zip counts
        zips_df = zips_df.rename(index=zip_ests)
        zips_df = zips_df.groupby(zips_df.index).sum()

        assert zips_df.index.isin(to_fips.index).all(), (
            'JData zips still missing from conversion table')
        return zips_df

    def to_dummies(self, orgs_df):
        """Replace JData org types and denominations with dummies."""
        df = pd.get_dummies(orgs_df, columns=['Type', 'Denom'])

        # join spaced column names
        df = df.rename(columns=lambda x: re.sub(r'\W', '_', x))

        return df

    def _orgs_to_zip_cnts(self, orgs_df):
        """Aggregate JData org counts by zip.

        Parameters
        ----------
        orgs_df : pandas.DataFrame

        Returns
        -------
        zips_df : pandas.DataFrame
            Each row indexed to a unique zip code with count
            aggregates for organizations that belong to it.
        """
        if not self.is_valid_zips(orgs_df.Zip):
            raise ValueError('Zip codes are not valid.')

        orgs_df = orgs_df.loc[:, ['Zip', 'Type', 'Denom']]
        zips_df = (self.to_dummies(orgs_df.fillna('None'))
                   .groupby('Zip').sum())
        return zips_df

    def _zip_cnts_to_fips(self, zips_df):
        """Aggregate org counts by zip to counts by county."""
        zips_df = self._missing_zip_to_nearest(zips_df, self.to_fips)

        to_fips_revelent = self.to_fips.loc[self.to_fips.index.isin(
            zips_df.index)]
        gb = to_fips_revelent.groupby(to_fips_revelent.index)

        # for each zip and its corresponding fips codes
        ls = []
        for zip_, fips_grp in gb:
            # if no other ratio (non-residential, non-business),
            # split zip counts across all counties, otherwise counts
            # would be zero'd out
            if not fips_grp.OTH_RATIO.sum():
                zero_ratio = True
                weight = 1 / len(fips_grp)
            elif np.isclose(fips_grp.OTH_RATIO.sum(), 1):
                zero_ratio = False
            else:
                raise ValueError('Ratios must total 1 to keep all orgs.')

            # iterate through each fips
            for zip_idx, row in fips_grp.iterrows():
                # Use imputed ratio if necessary
                weight = row['OTH_RATIO'] if not zero_ratio else weight

                new = zips_df.loc[zip_] * weight
                new.loc['FIPS'] = row.FIPS
                ls.append(new)

        cnty_df = pd.concat(ls, axis=1).T
        return cnty_df.groupby(cnty_df.FIPS).sum()

    def get_county_cnts(self, cats='all', excl_org_types=None):
        """Aggregate orgs into county counts of Denom and Type categories.

        Parameters
        ----------
        cats: str, default is 'all' which leave counts of both Denom and
            Type categoricals, while 'denom' and 'org_type' return
            counts of only one of the other.
        excl_org_types: None or list, None is default
            If list of org Type values, leave those out of count
            calculation.

        Returns
        -------
        self.cnty_df : DataFrame
        """
        self.orgs_df = self.orgs_df if excl_org_types is None else (
            self.orgs_df.loc[~self.orgs_df.Type.isin(excl_org_types)])
        self._zips_df = self._orgs_to_zip_cnts(self.orgs_df)
        self.cnty_df = self._zip_cnts_to_fips(self._zips_df)
        self.cnty_df = self._impute_denoms_county_cnts(self.cnty_df)

        if not self.is_valid_fips(self.cnty_df.index):
            raise ValueError('County FIPS codes index not valid')

        # correct for two categories counted
        total_cnts = self.cnty_df.sum().sum() / 2

        if not np.isclose(total_cnts, len(self.orgs_df)):
            raise ValueError(
                '{} total county counts does not match {} JData orgs'
                .format(total_cnts, len(orgs)))

        if cats == 'denom':
            self.cnty_df = self.cnty_df.select(
                lambda x: x.startswith('Denom_'), axis=1)
        elif cats == 'org_type':
            self.cnty_df = self.cnty_df.select(
                lambda x: x.startswith('Type_'), axis=1)
        elif cats != 'all':
            raise ValueError('cats arg must be either all, denom or type.')

        self.cnty_df = (self.cnty_df.rename_axis('FIPS', axis=0)
                        .loc[self.cnty_df.sum(1) != 0])
        return self.cnty_df

    def _impute_denoms_county_cnts(self, df):
        """
        Imputes missing denominations in county counts by
        spreading their counts across other denoms proportionally.

        NOTE: Only compatible when Denomination columns are prefixed
        with 'Denom_' like with cats='all' county county aggregation method.

        Parameters
        ----------
        df : pandas.DataFrame
            With only denomination count features
        """

        def weighted_dist(row, none_col):
            """
            Distributes counts of orgs with missing denoms across
            other denom counts in county accoring to proportion.
            """
            none_cnt = row[none_col]

            total = sum(row) - none_cnt
            if not total:
                row += none_cnt / (len(row) - 1)
            else:
                weights = row / total
                row += weights * none_cnt

            return row

        denom_cols = [col for col in df.columns if col.startswith('Denom_')]
        if not len(denom_cols):
            raise ValueError('No denomination columns found')

        none_col = [s for s in denom_cols if 'None' in s]
        if len(none_col) > 1:
            raise ValueError(
                'More than one None category count variable not supported')
        else:
            none_col = none_col[0]

        df.loc[df[none_col] > 0, denom_cols] = (
            df.loc[df[none_col] > 0, denom_cols].apply(
                weighted_dist, args=(none_col, ), axis=1))
        df = df.drop(none_col, axis=1)

        # drop counties that have no other Denom data than None
        has_only_none = df.loc[:, df.columns.isin(denom_cols)].sum(1) == 0
        df = df.loc[~has_only_none]

        return df

    def is_valid_zips(self, zips_):
        # since a few valid zips not present in conversion table
        return (~zips_.isin(self.to_fips.index)).sum() < 10

    def is_valid_fips(self, fips):
        return fips.isin(self.to_fips.FIPS).all()
