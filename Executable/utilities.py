import pandas as pd
from IPython.display import display
from scipy.stats import boxcox
from scipy.stats.mstats import zscore

DATA_DIR = '../Data/'

STATE_TO_ABBR_FP = ''.join([DATA_DIR, 'States-to-Abbrevs.csv'])
FIPS_CODES_FP = ''.join([DATA_DIR, 'Census-2010-County-FIPS.txt'])


def are_valid_state_abbrevs(df, st_col):
    """Check valid state abbrevs."""
    states = read_state_to_abbr()
    return df.loc[:, st_col].isin(states)


def display_cb(df, max_colwidth=80):
    """Display codebooks with long descriptions."""
    with pd.option_context('max_colwidth', max_colwidth):
        display(df)


def remove_state(col):
    """Remove trailing states from city/county feature."""
    return col.map(lambda x: ', '.join([x.strip() for x in x.split(',')][:-1]))


def split_state(df, col, suffix=None):
    """Splits state from city or county into a new col.

    States are then removed from target column and whole
    DataFrame is return.

    Parameters
    ----------
    df : pandas.DataFrame
        To process and return
    col : str
        Label of location column from which to split state off
    suffix : str
        To add to the new state column, separated by '_'
        If None: new column is named 'State'

    Returns
    -------
    df : pandas.DataFrame with additional column for state
    """
    state_col = 'State' if suffix is None else '_'.join([col, suffix])
    # Create state column
    df.loc[:, state_col] = df[col].map(lambda x: x.split(',')[-1].strip())
    df.loc[:, col] = remove_state(df[col])

    # readable order of city/county, state
    all_cols = df.columns.tolist()
    all_cols.insert(all_cols.index(col) + 1, state_col)
    df = df.reindex(columns=all_cols[:-1])

    return df


def read_state_to_abbr(fp=STATE_TO_ABBR_FP):
    """Read conversion table, state names to abbreviations."""
    return pd.read_csv(fp, index_col='State', squeeze=True)


def state_to_abbr(col, lookup_fp=STATE_TO_ABBR_FP):
    """Convert names of states to their abbreviations."""
    abbr_df = read_state_to_abbr(lookup_fp)
    abbrevs = col.map(lambda x: abbr_df[x.title()])
    return abbrevs


def code_to_str(col, width):
    """Convert FIPS to proper string format"""
    col = (col.astype(str).str.pad(width, side='left', fillchar='0'))
    return col


def read_fips_codes(fp):
    """Read 2010 Census County FIPS codes.

    Source: https://www.census.gov/geo/reference/codes/cou.html
    CODE is county FIPS code, a combination of state and county codes.

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
    """
    columns = ['STATE', 'STATEFP', 'COUNTYFP', 'COUNTYNAME', 'CLASSFP']
    df = pd.read_csv(fp, names=columns, header=None, dtype=str)
    df.loc[:, 'FIPS'] = df.STATEFP.str.cat(df.COUNTYFP)
    df = df.set_index('FIPS')

    return df


def fips_to_state_abbr(fips_col, fips_ref_fp):
    """Convert 5-digit FIPS county codes to State Abbreviations.

    Parameters
    ----------
    fips_col : Series of FIPS county codes.
    fips_ref_fp : str
        Filepath to 2010 Census County FIPS codes reference txt file.

    Returns
    -------
    abbr_col = Series of state abbreviations.
    """
    fips_codes = (read_fips_codes(fips_ref_fp)
                  .set_index('STATEFP')
                  .drop_duplicates('STATE')
                  .loc[:, 'STATE']
                  )
    abbr_col = fips_col.map(lambda x: fips_codes[x[:2]])

    return abbr_col


def is_outlier_val(col):
    """Detect outliers with Tukey's method.

    Technique determines an outlier if it fall outside
    the interquartile range expanded by 50 percent on either side.

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
    step = 1.5 * (q3 - q1)
    lo, hi = q1 - step, q3 + step

    result = (col < lo) | (col > hi)
    return result


def is_outlier_instance(data, thresh=2):
    """Determine outliers from multiple features.

    Parameters
    ----------
    df : pandas.DataFrame or pandas.Series
    thresh : int, default is 2
        Specifies how many outlier values must be present
        to identify an instance as an outlier.

    Returns
    -------
    is_outlier : pandas.Series, boolean
    """
    df = data.to_frame().T if isinstance(data, pd.Series) else data

    df = df.select_dtypes(['float', 'int'])

    is_outlier = df.apply(is_outlier_val)

    return is_outlier.sum(1) >= thresh


def rate_to_perc(df, rate_suffix, perc_suffix):
    """Convert rate variables to percentages."""
    for name, col in df.items():
        if name.endswith(rate_suffix):
            col /= 10
            new_name = name.replace(rate_suffix, perc_suffix)
            df = df.rename(columns={name: new_name})
    return df
