from jdata import read_jdata, extract_clean_jdata
from hud_geo_conversions import read_zips_to_fips

import numpy as np
import pandas as pd
import re

def missing_zip_to_nearest(jd_zip_cnts, zips_to_fips, msg=False):
    """Replace zip codes in df that are not present in
    zip-to-county conversion table. Replacements are
    estimates based on nearest ordinal proximity of zip.

    Parameters
    ----------
    jd_zip_cnts : pandas.DataFrame
        Must be indexed by zip code
    zips_to_fips : pandas.DataFrame
        Must have ZIP column, many-to-many zip-county reference
    msg : bool
        If true, print old and new zips for analysis

    Returns
    ----------
    df : pandas.DataFrame
        jd_zip_cnts with missing zips fixed
    """
    missing = set(jd_zip_cnts.index) - set(zips_to_fips.ZIP)
    zip_arr = zips_to_fips.ZIP.astype(int).values
    zip_ests = {}
    for zip_ in missing:
        idx = np.abs(zip_arr-int(zip_)).argmin()
        zip_est = str(zip_arr[idx]).zfill(5)
        zip_ests[zip_] = zip_est

    if msg:
        print('zips estimated for counties:')
        print(list(zip_ests.keys()))
        print('replaced with')
        print(list(zip_ests.values()))

    df = jd_zip_cnts.rename(index=zip_ests)
    return df

def impute_missing_oth_ratio(jd_zip_cnts, zips_to_fips):
    """Impute OTH_RATIO in ZIP-County Crosswalk.

    This arises when a particular zip is missing data on
    the ratio of locations across counties where there are
    multiple within a particular zip.
    (See crosswalk codebook for more details)
    """

    def imp_average(x):
        """If there are multiple counties within a zip,
        impute with simple average.
        """
        x.loc[:, 'OTH_RATIO'] = 1/len(x)
        return x

    relevant = zips_to_fips[zips_to_fips.ZIP.isin(jd_zip_cnts.index)]

    # impute where total OTH_RATIO across counties is zero
    # (i.e. is not close to 1)
    to_impute = relevant.groupby('ZIP').filter(lambda x: all(x.OTH_RATIO==0))
    imputed = to_impute.groupby('ZIP').apply(imp_average)
    zips_to_fips.loc[imputed.index] = imputed

    return zips_to_fips

def types_denoms_to_dummies(jdata, how='all', type_lbl='Type',
                            denom_lbl='Denom'):
    """Make dummy variables for JData org types and denominations.

    Parameters
    ----------
    jdata : pandas.DataFrame
    how : str
        'simple': aggregate with count of organizations
        'type', 'denom': aggregate with sum of dummies with
                         either categorical column
        'all': aggregate with sum of both categorical columns'
                dummies, i.e. two sets of dummy variables.
                Note that orgs will be counted twice, once for
                each category.
        'combined': like both, but dummies of derived from each
                    combination of type and denom
    type_lbl : str
        Name of column for organization types
    denom_lbl : str
        Name of column for organization denominations

    Returns
    -------
    jdata : pandas.DataFrame
        Passed DataFrame with Type and/or Denom columns replaced
        with dummy columns.
    """
    type_, denom = jdata[type_lbl], jdata[denom_lbl]
    if how=='type':
        source = type_
        to_drop = type_lbl
    if how=='denom':
        source = denom
        to_drop = denom_lbl
    if how=='combined':
        source = type_.str.cat(denom, sep='_')
        to_drop = [type_lbl, denom_lbl]
    if how=='all':
        source = jdata[[type_lbl, denom_lbl]]
        to_drop = [type_lbl, denom_lbl]

    jdata = pd.concat([jdata.drop(to_drop, axis=1),
                       pd.get_dummies(source)],
                      axis=1)
    return jdata

def aggr_jdata_to_zip(jdata, how='all', type_lbl='Type', denom_lbl='Denom'):
    """Aggregate JData org counts by zip.

    Parameters
    ----------
    jdata : pandas.DataFrame
    how : str
        'simple': aggregate with count of organizations
        'type', 'denom': aggregate with sum of dummies with
                         either categorical column
        'all': aggregate with sum of both categorical columns'
                dummies, i.e. two sets of dummy variables.
                Note that orgs will be counted twice, once for
                each category.
        'combined': like both, but dummies of derived from each
                    combination of type and denom
    type_lbl : str
        Name of column for organization types
    denom_lbl : str
        Name of column for organization denominations

    Returns
    -------
    jd_zip_cnts : pandas.DataFrame
        Each row indexed to a unique zip code with count
        aggregates for organizations that belong to it.
    """
    type_, denom = jdata[type_lbl], jdata[denom_lbl]
    assert all(~type_.isnull()) and all(~denom.isnull()), (
        'Type and/or denom columns have missing values.')

    how_params = ['simple', 'type', 'denom', 'all', 'combined']
    assert how in how_params, (
        'Invalid how parameter passed.')

    if how=='simple':
        zip_grps = jdata.groupby('Zip')
        jd_zip_cnts = zip_grps.Name.count().to_frame(name='Tot_Orgs')
    else:
        jdata = types_denoms_to_dummies(jdata, how=how, type_lbl=type_lbl,
                                        denom_lbl=denom_lbl)
        zip_grps = jdata.groupby('Zip')
        jd_zip_cnts = zip_grps.sum()

    return jd_zip_cnts

def jd_zip_cnts_to_county(jd_zip_cnts, zips_to_fips, how='all',
                          type_lbl='Type', denom_lbl='Denom'):
    """Converts counts of JData orgs by zip to county."""

    jd_zip_cnts = missing_zip_to_nearest(jd_zip_cnts, zips_to_fips)

    # relevant for weights, ensuring that some org don't get zero'd out
    zips_to_fips = impute_missing_oth_ratio(jd_zip_cnts, zips_to_fips)

    zips_to_fips = zips_to_fips.set_index('ZIP')
    df = jd_zip_cnts.join(zips_to_fips.FIPS)

    # ratio of non-business/non-residential locations
    # of a zip found in a particular county
    weights = zips_to_fips.loc[jd_zip_cnts.index, 'OTH_RATIO']
    df.iloc[:, :-1] = (df.iloc[:, :-1].apply(lambda x: x*weights))  # no COUNTY

    jd_county_cnts = df.groupby(df.index).sum()

    return jd_county_cnts

def jdata_to_county_cnts(jdata, zips_to_fips, how='all',
                         type_lbl='Type', denom_lbl='Denom'):

    jd_zip_cnts = aggr_jdata_to_zip(jdata, how=how, type_lbl=type_lbl,
                                    denom_lbl=denom_lbl)

    jd_county_cnts = jd_zip_cnts_to_county(jd_zip_cnts, zips_to_fips,
                                           how=how, type_lbl=type_lbl,
                                           denom_lbl=denom_lbl)

    jd_county_cnts = (jd_county_cnts.rename_axis('FIPS', axis=0)
                      .rename(columns=lambda x: re.sub(r'\W', '_', x))
                      .loc[jd_county_cnts.sum(1)!=0])

    return jd_county_cnts

def read_jdata_counties(jdata_fp, zips_to_fips_fp, how='all'):
    """Includes cleaning of JData org file only."""

    zips_to_fips = read_zips_to_fips(zips_to_fips_fp)

    jd_df = read_jdata(jdata_fp)
    jd_df = extract_clean_jdata(jd_df)

    jd_df = jd_df[  # day camps have no denom data
        (jd_df.Country!='CA') &
        (~jd_df.Type.isin(['Overnight camp', 'Day camp']))]

    jd_county = jdata_to_county_cnts(jd_df, zips_to_fips, how=how)

    # With col names 'None', can't access to the column through attribute
    jd_county = jd_county.rename(columns={'None': 'None_'})

    return jd_county

def impute_none_denoms(df, none_col):
    """
    Imputes missing denominations in county counts by
    spreading their counts across other denoms proportionally.

    Parameters
    ----------
    df : pandas.DataFrame
        wth only denomination count features
    none_col : str
        name of missing denomination count feature, can be included
        in denom_cols - won't make a difference.
    """

    # test code
    # print('Proportions maintained')
    # (jd_cnty.loc[:, jd_cnty.columns.isin(denom_cols)]
    #     .apply(lambda x: x/sum(x), axis=1).sum().sort_values())
    # if False:  # used to test spread_none_denoms()
    # data = read_jdata_counties(JDATA_FP, ZIP_TO_FIPS_FP, how='denom')
    # data = data.drop(['None_'], axis=1).apply(lambda x: x/sum(x), axis=1)
    # display(data.sum().sort_values())

    def weighted_dist(row, none_col):
        none_cnt = row[none_col]  # will drop None_ col at end
        total = sum(row) - none_cnt

        weights = row / total
        row += weights * none_cnt

        return row

    return (df.apply(weighted_dist, args=(none_col, ), axis=1)
            .drop([none_col], axis=1))

def merge_similar_denoms(df):

    if any('Denom_' in s for s in df.columns):
        DENOMS = dict(
            Denom_Orthodox=['Denom_Traditional', 'Denom_Sephardic'],
            Denom_Secular=['Denom_Humanistic']
            )
    else:
        DENOMS = dict(
            Orthodox=['Traditional', 'Sephardic'],
            Secular=['Humanistic']
            )

    for dest, denoms in DENOMS.items():
        df[dest] += df[denoms].sum(1)
        df = df.drop(denoms, axis=1)

    return df

def clean_jdata_county(df):

    none_col = [s for s in df.columns if 'None' in s]
    assert len(none_col) == 1, (
        'Cannot find unique or any feature for missing denominations')

    df = impute_none_denoms(df, none_col[0])
    df = merge_similar_denoms(df)

    return df
