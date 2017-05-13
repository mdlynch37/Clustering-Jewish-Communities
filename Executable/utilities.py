import pandas as pd
from IPython.display import display
from scipy.stats import boxcox
from scipy.stats.mstats import zscore

DATA_DIR = '../Data/'  # make null string if in same directory

def good_state_abbrevs(df, st_col):
    """Checks that state abbrevs are valid."""
    if st_col in df.columns:
        states = read_state_to_abbr()
        return df.loc[:, st_col].isin(states)
    else:
        return True  # no state column to check

def display_cb(df, max_colwidth=80):
    """Displays Dataframe with long descriptions.

    Used for codebooks.
    """
    with pd.option_context('max_colwidth', max_colwidth):
        display(df)

def remove_state(col):
    """Eliminates state from city/county.

    Robust to formats with multiple values preceeding state.
    """

    state_remover = lambda s: ', '.join([x.strip() for x in s.split(',')][:-1])
    return col.map(state_remover)

def split_state(df, col, suffix=None):
    """Splits state from city or county into a new col.

    States are then removed from target column and whole
    DataFrame is return.

    Parameters
    ----------
    df : pandas DataFrame
        To process and return
    col : str
        Label of location column from which to split state off
    suffix : str
        To add to the new state column, separated by '_'
        If None: new column is named 'State'

    Returns
    -------
    DataFrame with additional column for state
    """
    state_col = 'State' if suffix is None else '_'.join([col, suffix])

    state_splitter = lambda s: s.split(',')[-1].strip()
    df[state_col] = df[col].map(state_splitter)
    df[col] = remove_state(df[col])

    # readable order of city/county, state
    all_cols = df.columns.tolist()
    all_cols.insert(all_cols.index(col)+1, state_col)
    df = df.reindex(columns=all_cols[:-1])

    return df

def read_state_to_abbr(fp=''.join([DATA_DIR, 'States-to-Abbrevs.csv'])):
    """Reads conversion table for state names to abbreviations."""
    return pd.read_csv(fp, index_col='State', squeeze=True)

def state_to_abbr(col, lookup_fp=''.join([DATA_DIR, 'States-to-Abbrevs.csv'])):
    """Converts names of states to their abbreviations."""

    abbr_df = read_state_to_abbr(lookup_fp)
    abbrevs = col.map(lambda x: abbr_df[x.title()])
    return abbrevs

def code_to_str(col, width):
    """Converts FIPS to proper string format"""

    col = (col.astype(str)
           .str.pad(width, side='left', fillchar='0')
           )
    return col

def read_fips_codes(fp='Census-2010-County-FIPS.txt'):
    """Reads 2010 Census County FIPS codes.

    CODE column is combination of state and county codes.

    STATE: State Postal Code
        e.g. 'FL'
    STATEFP: State FIPS Code
        e.g. '12'
    COUNTYFP: County FIPS Code
        e.g. '011'
    COUNTYNAME: County Name and Legal/Statistical Area Description
        e.g. 'Broward County'
    CLASSFP: FIPS Class Code
        e.g. H1, see below for more details

    FIPS Class Codes
        H1: identifies an active county or statistically equivalent
            entity that does not qualify under subclass C7 or H6.
        H4: identifies a legally defined inactive or nonfunctioning
            county or statistically equivalent entity that does not
            qualify under subclass H6.
        H5: identifies census areas in Alaska, a statistical county
            equivalent entity.
        H6: identifies a county or statistically equivalent entity
            that is areally coextensive or governmentally consolidated
            with an incorporated place, part of an incorporated place,
            or a consolidated city.
        C7: identifies an incorporated place that is an independent city;
            that is, it also serves as a county equivalent because it is
            not part of any county, and a minor civil division (MCD)
            equivalent because it is not part of any MCD.

    Source: https://www.census.gov/geo/reference/codes/cou.html
    """
    columns = ['STATE', 'STATEFP', 'COUNTYFP', 'COUNTYNAME', 'CLASSFP']
    df = pd.read_csv(fp, names=columns, header=None, dtype=str)
    df['FIPS'] = df.STATEFP + df.COUNTYFP
    df = df.set_index('FIPS')

    return df

def is_outlier_val(col):
    """Detects outliers using Tukey's method.

    Technique determines an outlier if it fall outside
    the interquartile range expanded by 50% on either side.

    Parameters
    ----------
    col : pandas.Series
        Specific feature to be assessed

    Returns
    -------
    result : pandas.Series
        Of boolean values, true if value is outlier
    """
    q1, q3 = col.quantile(q=.25), col.quantile(q=.75)
    step = 1.5 * (q3-q1)
    lo, hi = q1-step, q3+step

    result = (col<lo) | (col>hi)
    return result

def is_outlier_instance(df, thresh=2, col_func=is_outlier_val):
    """Determines outliers from multiple features.

    Parameters
    ----------
    df : pandas.DataFrame or pandas.Series
    thresh : int, default=2
        Specifies how many outlier values must be present
        to identify an instance as an outlier.

    Returns
    -------
    is_outlier : pandas.Series, boolean
    """
    if type(df)==pd.core.series.Series:
        df = df.to_frame()

    df = df.select_dtypes(['float', 'int'])
    thresh = df.shape[1] if df.shape[1] < thresh else thresh

    is_outlier = df.apply(col_func)
    is_outlier = is_outlier.sum(axis=1) >= thresh

    return is_outlier

def boxcox_standardize(df, scale=True):
    """Scale and standardize sparse data with Box-Cox algorithm."""
    cols = []
    numeric = df.select_dtypes([float, int]) + 1 # corrects for 0s
    for name, col in numeric.iteritems():
        vals, _ = boxcox(col)
        vals = zscore(vals) if scale else vals
        cols.append(pd.Series(vals, name=name))

    df = pd.concat(cols, axis=1)
    return df

def rate_to_perc(df, rate_suffix, perc_suffix):
    """Converts rate variables to percentages."""
    for name, col in df.items():
        if name.endswith(rate_suffix):
            col /= 10
            new_name = name.replace(rate_suffix, perc_suffix)
            df = df.rename(columns={name: new_name})
    return df
