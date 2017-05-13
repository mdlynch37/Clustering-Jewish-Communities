from collections import OrderedDict
import re

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


STANDARD_COLS = OrderedDict([('STABBR', 'State'),
                             ('CNTYNAME', 'County'),
                             ('TOTCNG', 'Tot_Cngs'),
                             ('TOTADH', 'Tot_No'),
                             ('TOTRATE', 'Tot_Ra')])

JUDHAISM_COLS = OrderedDict([('CJUDCNG', 'ConsvJud_Cngs'),
                             ('CJUDADH', 'ConsvJud_No'),
                             ('CJUDRATE', 'ConsvJud_Ra'),
                             ('OJUDCNG', 'OrthJudCngs'),
                             ('OJUDADH', 'OrthJud_No'),
                             ('OJUDRATE', 'OrthJud_Ra'),
                             ('RJUDCNG', 'ReconJud_Cngs'),
                             ('RJUDADH', 'ReconJud_No'),
                             ('RJUDRATE', 'ReconJud_Ra'),
                             ('RFRMCNG', 'RefJud_Cngs'),
                             ('RFRMADH', 'RefJud_No'),
                             ('RFRMRATE', 'RefJud_Ra'),
                             ('UMJCCNG', 'UnionMessJews_Cngs')])

TO_FRONT = ['FIPS', 'STCODE', 'CNTYCODE', 'CNTYNAME',
            'STABBR', 'STNAME', 'POP2010']

def reorder_cols(df, column=True):
    """Reorder and rename vars."""

    df = df if column else df.T
    order = TO_FRONT + df.drop(TO_FRONT, axis=1).columns.tolist()
    df = df.loc[:, order]
    df = df if column else df.T

    return df

def read_cb(fp):
    """
    Reads codebook .txt file for U.S. Religion Census,
    2010 County File.
    """
    with open(fp) as f:
        cb_txt = f.read()
    data = re.findall(r'\d+[)][ ]([^\n\r]+)\s+([^\n\r]+)', cb_txt)
    df = (pd.DataFrame(data, columns=['VAR', 'DESCRIPTION'])
          .set_index('VAR')
          )

    df = reorder_cols(df, column=False)

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

        df.FIPS     = code_to_str(df.FIPS, 5)
        df.STCODE   = code_to_str(df.STCODE, 2)
        df.CNTYCODE = code_to_str(df.CNTYCODE, 3)

        return df

    df = (pd.read_stata(fp)
          .rename(columns=lambda x: x.upper())
          .pipe(pad_codes)
          .pipe(reorder_cols)
          .fillna(0)
          .set_index('FIPS')
          )
    return df

def read_judaic_denoms(fp):
    """Extracts only data for Judaic denominations."""

    old_to_new = STANDARD_COLS.copy()
    old_to_new.update(JUDHAISM_COLS)

    return (read_all_denoms(fp)
            .rename(columns=old_to_new)
            .loc[:, old_to_new.values()]
            .dropna(how='all', subset=JUDHAISM_COLS.values())
            )
