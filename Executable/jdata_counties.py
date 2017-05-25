import math
import re

import numpy as np
import pandas as pd

from hud_geo_conversions import read_zips_to_fips
from jdata import JData


class JDataCounties(JData):
    """Counts of JData orgs for US counties by type and denomination."""

    def __init__(self, jdata_org_fp, zip_to_fips_fp):
        """
        Read in supporting zip-fips conversion table.

        Parameters
        ----------
        jdata_org_fp : str
            Filepath to jdata json file
        zip_to_fips_fp : str
            Filepath to HUD Zip-FIPS code conversion table (.xlsx)

        Attributes
        ----------
        orgs_df : DataFrame of individual organizations
        _zip_cnts_df : DataFrame, intermediate step of Zip counts
            before aggregating further to counties
        cnty_df : DataFrame of organization county indexed to
            FIPS county codes used by US Census Bureau
        """
        self.zip_to_fips_fp = zip_to_fips_fp
        self.jdata_org_fp = jdata_org_fp

        self.to_fips = read_zips_to_fips(zip_to_fips_fp)

        # Intermediate steps.
        # If client makes changes to self.orgs_df, those are preserved
        # whereas self._zip_cnts_df is re-aggregated.
        self.orgs_df = None
        self._zip_cnts_df = None

        self.cnty_df = None

    def get_county_cnts(self,
                        categorical='both',
                        exclude=None,
                        combine_denoms=True):
        """Aggregate orgs into county counts of Denom and Type categories.

        Parameters
        ----------
        categorical : str, default is 'both' which leave counts of
            Denom and Type categoricals, while 'denom' and 'org_type'
            return values counts of only one or the other.
            Note: This will not affect count aggregation.
        exclude : None or list, default is None
            List specifies values of Denom and/or Type categoricals
            to exclude (not prepended with Denom_ or Type_.
            If both Denoms and Types are specified,
            any organization that has either value will be discarded.
            Note: This will affect count aggregation.
        combine_denoms : bool, default is True
            Passed to read_orgs

        Returns
        -------
        self.cnty_df : DataFrame
        """
        if self.orgs_df is None:
            # orgs_df must be cleaned to be compatible with
            # get_county_cnts()
            self.orgs_df = self.read_orgs(
                self.jdata_org_fp, clean=True, combine_denoms=combine_denoms)

        orgs_df = self.orgs_df  # so self.orgs_df state preserved

        if exclude is not None:
            valid_vals = (orgs_df.Type.unique().tolist() +
                          orgs_df.Denom.unique().tolist())
            if any(x not in valid_vals for x in exclude):
                raise ValueError('Values to be excluded must be in data.')

            orgs_df = orgs_df.loc[~orgs_df[['Type', 'Denom'
                                            ]].isin(exclude).any(1)]
        self._zip_cnts_df = self._orgs_to_zip_cnts(orgs_df)
        self.cnty_df = (self._zip_cnts_to_fips(self._zip_cnts_df)
                             .pipe(self._impute_denoms_county_cnts))

        if categorical == 'denom':
            self.cnty_df = self.cnty_df.select(
                lambda x: x.startswith('Denom_'), axis=1)
        elif categorical == 'type':
            self.cnty_df = self.cnty_df.select(
                lambda x: x.startswith('Type_'), axis=1)
        elif categorical != 'both':
            raise ValueError(
                'categorical arg must be either all, denom or type.')

        self.cnty_df = (self.cnty_df.rename_axis('FIPS', axis=0)
                             .loc[self.cnty_df.sum(1) != 0])

        # Validate FIPS index
        if not self.cnty_df.index.isin(self.to_fips.FIPS).all():
            raise ValueError('County FIPS codes index not valid')

        # Validate total organizations counted
        norm = 2 if categorical is 'both' else 1  # if orgs counted twice
        total_cnts = self.cnty_df.values.sum() / norm
        if not math.isclose(total_cnts, len(orgs_df)):
            raise ValueError(
                '{} total county counts does not match {} JData orgs'
                .format(total_cnts, len(orgs_df)))

        return self.cnty_df

    def _orgs_to_zip_cnts(self, orgs_df):
        """Aggregate JData org counts by zip.

        Parameters
        ----------
        orgs_df : pandas.DataFrame

        Returns
        -------
        zip_cnts_df : pandas.DataFrame
            Each row indexed to a unique zip code with count
            aggregates for organizations that belong to it.
        """
        # Less than 10 valid zips not present in conversion table
        if not~orgs_df.Zip.isin(self.to_fips.index).sum() < 10:
            raise ValueError('Zip codes are not valid.')

        orgs_df = orgs_df.loc[:, ['Zip', 'Type', 'Denom']]
        zip_cnts_df = (self._to_dummies(orgs_df.fillna('None'))
                       .groupby('Zip').sum())
        return zip_cnts_df

    def _zip_cnts_to_fips(self, zip_cnts_df):
        """Aggregate org counts by zip to counts by county."""
        zip_cnts_df = self._missing_zip_to_nearest(zip_cnts_df, self.to_fips)

        to_fips_revelent = self.to_fips.loc[self.to_fips.index.isin(
            zip_cnts_df.index)]
        gb = to_fips_revelent.groupby(to_fips_revelent.index)

        # for each zip and its corresponding fips codes
        ls = []
        for zip_, fips_grp in gb:
            # if no other ratio (non-residential, non-business),
            # split zip counts evenly across counties, otherwise counts
            # would be zero'd out
            if np.isclose(fips_grp.OTH_RATIO.sum(), 1):
                needs_imputed = False
            elif fips_grp.OTH_RATIO.sum() == 0:
                needs_imputed = True
                imputed_ratio = 1 / len(fips_grp)
            else:
                raise ValueError('Ratios must total 1 to keep all orgs.')

            # iterate through each county group with the zips that
            # comprise it, taking counts of each zip and weighting
            # them by the fraction that they exist in the county
            for zip_idx, row in fips_grp.iterrows():
                # Use imputed ratio if necessary
                weight = imputed_ratio if needs_imputed else row.OTH_RATIO

                new = zip_cnts_df.loc[zip_] * weight
                new.loc['FIPS'] = row.FIPS
                ls.append(new)

        cnty_df = pd.concat(ls, axis=1).T
        return cnty_df.groupby(cnty_df.FIPS).sum()

    @staticmethod
    def _missing_zip_to_nearest(zip_cnts_df, to_fips, msg=False):
        """
        Replace zip codes in df that are not present in
        zip-to-county conversion table. Replacements are
        estimates based on nearest ordinal proximity of zip.

        Parameters
        ----------
        zip_cnts_df : pandas.DataFrame, must be indexed by zip code
        to_fips : pandas.DataFrame, must have index of ZIPS,
                  one-to-many zip-county conversion table
        msg : bool, if true, print old and new zips for analysis

        Returns
        ----------
        df : pandas.DataFrame, zip_cnts with missing zips fixed
        """
        missing = set(zip_cnts_df.index) - set(to_fips.index)
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
        zip_cnts_df = zip_cnts_df.rename(index=zip_ests)
        zip_cnts_df = zip_cnts_df.groupby(zip_cnts_df.index).sum()

        assert zip_cnts_df.index.isin(to_fips.index).all(), (
            'JData zips still missing from conversion table')
        return zip_cnts_df

    @staticmethod
    def _to_dummies(orgs_df):
        """Replace JData org types and denominations with dummies."""
        df = pd.get_dummies(orgs_df, columns=['Type', 'Denom'])

        # join spaced column names
        df = df.rename(columns=lambda x: re.sub(r'\W', '_', x))

        return df

    @staticmethod
    def _impute_denoms_county_cnts(df):
        """
        Imputes missing denominations in county counts by
        spreading their counts across other denoms proportionally.

        NOTE: Only compatible when Denomination columns are prefixed
        with 'Denom_' like with categorical='both' county county
        aggregation method.

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


if __name__ == '__main__':
    DATA_DIR = '../Data/'
    SCL_DIR = ''.join([DATA_DIR, 'Schools/'])

    ZIPS_TO_FIPS_FP = ''.join([DATA_DIR, 'ZIP_COUNTY_122016.xlsx'])
    JDATA_FP = ''.join([SCL_DIR, 'jdata_directory.json'])

    jdc = JDataCounties(JDATA_FP, ZIPS_TO_FIPS_FP)
    cnty_df = jdc.get_county_cnts(clean=True, combine_denoms=True)
