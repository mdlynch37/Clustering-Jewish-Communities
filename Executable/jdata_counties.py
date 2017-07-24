import math
import re

import numpy as np
import pandas as pd

from hud_geo_conversions import read_zips_to_fips
from jdata import JData
from utilities import read_fips_codes

# TODO: Override read_orgs to add county estimates to directory

# To reorder from most conservative to least (or thereabouts),
# for the sake of consistency across datasets
DENOM_COLS = ['Denom_Orth', 'Denom_Seph', 'Denom_Trad', 'Denom_Consv',
              'Denom_Recon', 'Denom_Ref', 'Denom_Comm', 'Denom_PlurTrans',
              'Denom_Hum', 'Denom_Sec', 'Denom_Oth', 'Denom_NonDenom',
              'Denom_Zionist']
TYPE_COLS  = ['Type_DaySch', 'Type_PTSch', 'Type_EarlyChild', 'Type_DayCamp',
              'Type_OverCamp']

DATA_DIR    = '../Data/'
FIPS_CODES_FP = ''.join([DATA_DIR, 'Census-2010-County-FIPS.txt'])


class JDataCounties(JData):
    """Counts of JData orgs for US counties by type and denomination."""

    def __init__(self, jdata_org_fp, zips_to_fips_fp):
        """
        Read in supporting zip-fips conversion table.

        Parameters
        ----------
        jdata_org_fp : str
            Filepath to jdata json file
        zips_to_fips_fp : str
            Filepath to HUD Zip-FIPS code conversion table (.xlsx)

        Attributes
        ----------
        orgs_df : DataFrame of individual organizations
        _zip_cnts_df : DataFrame, intermediate step of Zip counts
            before aggregating further to counties
        cnty_df : DataFrame of organization county indexed to
            FIPS county codes used by US Census Bureau
        """
        self.zip_to_fips_fp = zips_to_fips_fp
        self.jdata_org_fp = jdata_org_fp

        self.to_fips = read_zips_to_fips(zips_to_fips_fp)

        # Intermediate steps.
        # If client makes changes to self.orgs_df, those are preserved
        # whereas self._zip_cnts_df is re-aggregated.
        self.orgs_df = None
        self._zip_cnts_df = None

        # Under-the-hood self.orgs_df that stores the final version
        # that is aggregated into county counts based on filtering options.
        # Used for testing.
        self._orgs_df = None

        self.cnty_df = None

        self.include = None
        self.exclude = None

    def read_orgs(self, fp):
        """Restrict orgs to cleaned and American.
        
        County aggregation breaks otherwise.
        """
        super().read_orgs(fp, clean=True, only_usa=True)

        return self.orgs_df

    @staticmethod
    def _filter_incl_excl(orgs_df, include, exclude):
        """Select orgs that match include or exclude criteria."""
        valid_types = orgs_df.Type.unique().tolist()
        valid_denoms = orgs_df.Denom.unique().tolist()
        valid_vals = valid_types + valid_denoms

        if exclude is not None:
            if isinstance(exclude, str):
                exclude = [exclude]
            if any(x not in valid_vals for x in exclude):
                raise ValueError('Values to be excluded must be in data.')

            orgs_df = orgs_df.loc[
                ~orgs_df[['Type', 'Denom']].isin(exclude).any(1)
            ]
        # Including has a more complicated logic. If only Denoms
        # are specified, it is assumed all Types are included, and
        # vice versa.
        if include is not None:
            if isinstance(include, str):
                include = [include]
            if any(x not in valid_vals for x in include):
                raise ValueError('Values to be included must be in data.')

            include_type = any(x in valid_types for x in include)
            include_denom = any(x in valid_denoms for x in include)
            if include_type and include_denom:
                orgs_df = orgs_df.loc[
                    orgs_df[['Type', 'Denom']].isin(include).all(1)
                ]
            elif include_type or include_denom:
                orgs_df = orgs_df.loc[
                    orgs_df[['Type', 'Denom']].isin(include).any(1)
                ]
        return orgs_df

    @staticmethod
    def filter_categorical(cnty_df, categorical):
        """Drop categorical columns if necessary."""
        if categorical == 'denom':
            cnty_df = cnty_df.select(
                lambda x: x.startswith('Denom_'), axis=1
            )
        elif categorical == 'type':
            cnty_df = cnty_df.select(
                lambda x: x.startswith('Type_'), axis=1
            )
        elif categorical != 'both':
            raise ValueError(
                'categorical arg must be either both, denom or type.')

        return cnty_df

    def get_county_cnts(self,
                        categorical='both',
                        include=None,
                        exclude=None,
                        reload_orgs=False):
        """Aggregate orgs into county counts of Denom and Type categories.

        Parameters
        ----------
        categorical : str, default is 'both' which leave counts of
            Denom and Type categoricals, while 'denom' and 'org_type'
            return values counts of only one or the other.
            Note: This will not affect count aggregation.
        include, exclude : None, list or str, default is None
            Specifies one or more values of Denom and/or Type categoricals
            to include or exclude (not prepended with Denom_ or Type_.
            Both include and exclude values cannot be specified.
            If both Denoms and Types values are *include*,
            any organization that meets *both* criteria will be kept.
            If both Denoms and Types values are *excluded*,
            any organization that meets *either* criteria will be dropped.
            Note: This filtering will affect count aggregation.
        reload_orgs : bool
            Overwrites state of orgs_df attribute, discarding any changes
            the client made to it directly.

        Returns
        -------
        self.cnty_df : DataFrame
        """
        self.include = include
        self.exclude = exclude
        
        # If orgs_df already loaded, using bound DataFrame allows
        # changes to it to be used in count calculation.
        # If the client wants a clean read, flag reload_orgs.
        if self.orgs_df is None or reload_orgs:
            # orgs_df must be cleaned to be compatible with
            # get_county_cnts(). Only American orgs are supported.
            self.orgs_df = self.read_orgs(self.jdata_org_fp)

        # Method may modify orgs for count calculation, but
        # self.orgs_df state will be maintained.
        self._orgs_df = self.orgs_df.copy()

        if include is not None and exclude is not None:
            raise ValueError('Cannot pass both include and exclude kwargs.')

        elif include is not None or exclude is not None:
            self._orgs_df = self._filter_incl_excl(self._orgs_df, include, exclude)
            
        self._zip_cnts_df = self._orgs_to_zip_cnts(self._orgs_df)
        self.cnty_df = (self._zip_cnts_to_fips(self._zip_cnts_df)
                        .pipe(self._impute_denoms_county_cnts))

        self.cnty_df = self.filter_categorical(self.cnty_df, categorical)

        # Validate FIPS index
        if not self.cnty_df.index.isin(self.to_fips.FIPS).all():
            raise ValueError('County FIPS codes index not valid')

        # Validate total organizations counted
        norm = 2 if categorical is 'both' else 1  # if orgs counted twice
        total_cnts = self.cnty_df.values.sum() / norm
        source_ref = len(self._orgs_df)
        if not math.isclose(total_cnts, source_ref):
            raise ValueError(
                '{} total county counts does not match {} JData orgs'
                .format(total_cnts, source_ref))

        # Validate aggregated state counts
        # state_cnts = df.groupby(fips_codes.STATE).apply(len)
        # source_ref   = orgs_df.groupby('State').apply(len)

        # reorder columns
        reordered = [col for col in TYPE_COLS + DENOM_COLS
                     if col in self.cnty_df.columns]
        self.cnty_df = self.cnty_df.reindex(columns=reordered)

        self.cnty_df = self.cnty_df.rename_axis('FIPS', axis=0)
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
        if orgs_df.Zip.isin(self.to_fips.index).sum() < 10:
            raise ValueError('Zip codes are not valid.')

        orgs_dum = pd.get_dummies(orgs_df[['Type', 'Denom']], dummy_na=True)
        zip_cnts_df = orgs_dum.groupby(orgs_df.Zip).sum()
        zip_cnts_df = zip_cnts_df.rename(
            columns=lambda x: re.sub(r'\W', '_', x)
        )
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
        zip-to-county conversion table. Replacements are rough
        estimates based on nearest ordinal proximity of zip.

        Parameters
        ----------
        zip_cnts_df : pandas.DataFrame, must be indexed by zip code
        to_fips : pandas.DataFrame, must have index of ZIPS,
                  many-to-many zip-county conversion table
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
        elif len(none_col) == 1:
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
    from religion_census import read_judaic_cngs

    DATA_DIR = '../Data/'

    SCL_DIR = ''.join([DATA_DIR, 'Schools/'])
    REL_DIR = ''.join([DATA_DIR, 'Religion/'])

    ZIPS_TO_FIPS_FP = ''.join([DATA_DIR, 'ZIP_COUNTY_122016.xlsx'])
    RELCEN_FP = ''.join([REL_DIR, 'US-Religion-Census-2010-County-File.DTA'])
    JDATA_FP = ''.join([SCL_DIR, 'jdata_directory.json'])

    def one_org_or_more_counties(relcen_fp, jdata_fp, zips_to_fips_fp):
        """Return Series of counties with one or more Synagogues or
        JData organizations.
        """
        denoms = ['Orth', 'Consv', 'Trad', 'Seph', 'Ref', 'Recon', 'Comm']
        # denoms = []
        types = ['DaySch', 'PTSch', 'EarlyChild']
        #     types   = ['DaySch', 'EarlyChild']
        jd_orgs = (JDataCounties(jdata_fp, zips_to_fips_fp)
                   .get_county_cnts('type', include=denoms + types)
                   )
        synags = read_judaic_cngs(relcen_fp, standard_cols=False)
        totals = (jd_orgs.join(synags, how='outer')
                  .dropna(how='any', axis=0)
                  #                .sum(1)
                  )
        return totals


    tots = one_org_or_more_counties(RELCEN_FP, JDATA_FP, ZIPS_TO_FIPS_FP)
