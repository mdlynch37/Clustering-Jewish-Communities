import pandas as pd

from utilities import code_to_str

DATA_DIR = '../Data/'  # empty string if data in same directory
FIPS_ZIPS_FP = ''.join([DATA_DIR, 'COUNTY_ZIP_122016.xlsx'])
ZIPS_FIPS_FP = ''.join([DATA_DIR, 'ZIP_COUNTY_122016.xlsx'])

def read_zips_to_fips(fp=None, inverse=False):
    """
    Reads 2010 Census counties to USPS Zips crosswalk file,
    updated for Q4 2016.

    Compiled by Office of Policy Development and Research (PD&R),
    derived from quarterly USPS Vacancy Data.

    Original filenames: 'ZIP_COUNTY_122016.xlsx',
                        'COUNTY_ZIP_122016.xlsx'

    From the documentation: "HUD is unable to geocode a small number of
    records that we receive from the USPS.". A solution is to use the
    impute_counties function in this module to replace those missing
    zip codes with their nearest integer neighbor. This is done best
    done when needed, i.e. when this file is used for aggregation etc.

    Columns:
        RES_RATIO: Proportion of residential addresses in the ZIP that
            that comprise a given county.
        BUS_RATIO: Proportion of business addresses in the ZIP that
            that comprise a given county.
        OTH_RATIO: Proportion of other addresses in the ZIP that
            that comprise a given county.
        TOT_RATIO: Proportion of all addresses in the ZIP that
            that comprise a given county.
    Source:
        https://www.huduser.gov/portal/datasets/usps_crosswalk.html


    Parameters
    ----------
    fp : str
        Filepath to .xlsx file. Default None will look for file
        with original name in same directory
    inverse : bool
        Default is False. If True, convert counties to zips.

    Returns
    -------
    df : pandas.DataFrame
    """

    if fp is None:
        if not inverse:
            fp = ZIPS_FIPS_FP
        else:
            fp = FIPS_ZIPS_FP

    df = pd.read_excel(fp)
    df.ZIP = code_to_str(df.ZIP, 5)
    df = df.rename(columns=dict(COUNTY='FIPS'))
    df.FIPS = code_to_str(df.FIPS, 5)

    return df
