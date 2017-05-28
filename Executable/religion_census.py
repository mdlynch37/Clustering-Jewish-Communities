import re
from collections import OrderedDict

import pandas as pd

from utilities import code_to_str

"""
STANDARD_COLS, JUDHAISM_COLS : OrderedDict, list or None
    OrderedDict used to filter then rename and order fields.
    List to leave field names as is. None returns original fields.
    Standard fields are metadata (County names, etc.) and aggregate
    data (totals).
    Relevant fields are for data on specific denominations.
    This distinction allows removing counties that do not have
"""

STANDARD_COLS = OrderedDict([
    ('STABBR', 'State'),
    ('CNTYNAME', 'County'),
    ('TOTCNG', 'Tot_Cngs'),
    ('TOTADH', 'Tot_No'),
    ('TOTRATE', 'Tot_Ra')
])

# Reorder from most conservative to least (or thereabouts),
# for the sake of consistency across datasets
JUDHAISM_COLS = OrderedDict([
    ('OJUDCNG', 'OrthJud_Cngs'),
    ('OJUDADH', 'OrthJud_No'),
    ('OJUDRATE', 'OrthJud_Ra'),
    ('CJUDCNG', 'ConsvJud_Cngs'),
    ('CJUDADH', 'ConsvJud_No'),
    ('CJUDRATE', 'ConsvJud_Ra'),
    ('RFRMCNG', 'RefJud_Cngs'),
    ('RFRMADH', 'RefJud_No'),
    ('RFRMRATE', 'RefJud_Ra'),
    ('RJUDCNG', 'ReconJud_Cngs'),
    ('RJUDADH', 'ReconJud_No'),
    ('RJUDRATE', 'ReconJud_Ra'),
    ('UMJCCNG', 'UnionMessJews_Cngs')
])

TO_FRONT = [
    'FIPS', 'STCODE', 'CNTYCODE', 'CNTYNAME', 'STABBR', 'STNAME', 'POP2010'
]


def reorder_general_cols(df, index=True):
    """Reorder and rename vars."""

    # column when passing codebook with variables in index
    df = df if index else df.T
    order = TO_FRONT + df.drop(TO_FRONT, axis=1).columns.tolist()
    df = df.reindex(columns=order)
    df = df if index else df.T

    return df


def read_cb(fp):
    """
    Reads codebook .txt file for U.S. Religion Census,
    2010 County File.
    """
    with open(fp) as f:
        cb_txt = f.read()
    data = re.findall(r'\d+[)][ ]([^\n\r]+)\s+([^\n\r]+)', cb_txt)
    df = (pd.DataFrame(data, columns=['VAR', 'DESCRIPTION']).set_index('VAR'))

    df = reorder_general_cols(df, index=False)

    return df


def read_all_denoms(fp):
    """Reads U.S. Religion Census, 2010 County File.

    Religious Congregations and Membership Study,
    not associated with US goverment census.

    Source:
    http://www.thearda.com/Archive/Files/Descriptions/RCMSCY10.asp

    Parameters
    ----------
    fp : str
        filepath to Stata .DTA file

    Returns
    -------
    df : pandas.DataFrame
    """

    def pad_codes(df):
        """Convert code cols to strings and pad."""

        df.FIPS = code_to_str(df.FIPS, 5)
        df.STCODE = code_to_str(df.STCODE, 2)
        df.CNTYCODE = code_to_str(df.CNTYCODE, 3)

        return df

    df = (pd.read_stata(fp)
          .rename(columns=lambda x: x.upper())
          .pipe(pad_codes)
          .pipe(reorder_general_cols)
          .fillna(0)
          .set_index('FIPS')
    )
    return df


def read_judaic_relcen(fp, how='all', standard_cols=False):
    """Extracts only data for Judaic denominations.
    
    Parameters
    ----------
    fp : str, filepath
    how : string, indicates which types of data to return
        Default 'all' leaves all data in place
        'cngs' only returns congregation data
        'adhs' only returns membership data
        'rate' only returns rate data, i.e. adherents per 
               1000 of total population.
    """

    if standard_cols:
        cols = STANDARD_COLS.copy()
        cols.update(JUDHAISM_COLS)
    else:
        cols = JUDHAISM_COLS

    df = (read_all_denoms(fp)
          .rename(columns=cols)
          .loc[:, cols.values()]
    )
    # drop empty
    df = df.loc[df.select(lambda col: col in JUDHAISM_COLS.values(), axis=1)
                    .sum(1) > 0]

    # Reorder from most conservative to least (or thereabouts),
    # for the sake of consistency across datasets
    df = df.loc[:, cols.values()]

    if how != 'all':
       if how == 'cngs':
           suffix = '_Cngs'
       elif how == 'adhs':
           suffix = '_No'
       elif how == 'rate':
           suffix = '_Ra'
       else:
           raise ValueError('Invalid how parameter passed.')

       df = df.select(
           lambda x: x.endswith(suffix) or x in STANDARD_COLS.values(), axis=1)

    return df
