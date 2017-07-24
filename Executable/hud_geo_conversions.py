import pandas as pd

from utilities import code_to_str


def read_zips_to_fips(fp, inverse=False):
    """
    Read 2010 Census counties to USPS Zips crosswalk file,
    updated for Q4 2016.

    Compiled by Office of Policy Development and Research (PD&R),
    derived from quarterly USPS Vacancy Data.
    Source: https://www.huduser.gov/portal/datasets/usps_crosswalk.html

    Filenames: 'ZIP_COUNTY_122016.xlsx',
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

    Parameters
    ----------
    fp : str
        Filepath to .xlsx file.
    inverse : bool, default is False
        If True, read county-to-zip table instead of default
        and set index to FIPS instead of ZIP

    Returns
    -------
    df : pandas.DataFrame
    """
    # Guard against inverse flag passed with wrong conversion table.
    # This can be hard to notice otherwise since Zips and FIPS codes
    # have same 5-digit string format.
    if inverse and fp.split('/')[-1] != 'COUNTY_ZIP_122016.xlsx':
        raise ValueError('Inverse should load file: COUNTY_ZIP_122016.xlsx')
    if not inverse and fp.split('/')[-1] != 'ZIP_COUNTY_122016.xlsx':
        raise ValueError('Should load file: ZIP_COUNTY_122016.xlsx')

    df = pd.read_excel(fp)
    df = df.rename(columns={'COUNTY': 'FIPS'})

    df.ZIP = code_to_str(df.ZIP, 5)
    df.FIPS = code_to_str(df.FIPS, 5)

    df = df.set_index('FIPS') if inverse else df.set_index('ZIP')

    return df
