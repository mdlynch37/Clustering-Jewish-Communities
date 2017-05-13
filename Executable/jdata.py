import pandas as pd

def manual_imputes(df):
    """
    Manual fixes to null location values, for analysis
    of US counties with small Jewish populations.
    """
    df.loc[df.State.isin(['QC', 'ON']), 'Country'] = 'CA'

    df.loc[260, ['Zip', 'Country']] = '35031', 'US'
    df.loc[347, ['City', 'State', 'Zip', 'Country']] = (
        'Fallsburg', 'NY', '12733', 'Country')
    df.loc[696, ['State', 'Zip', 'Country']] = 'PA', '15217', 'US'
    df.loc[860, ['City', 'Zip', 'Country']] = 'Kahului', '96732', 'US'
    df.loc[1418, ['City', 'State', 'Zip', 'Country']] = (
        'Mentor', 'OH', '44060', 'US')

    df = df.drop([386, 2084, 2142, 2143])

    return df

def read_jdata(fp):
    """Reads JData directory of educational orgs.

    Directory of Jewish day camps, day schools, early childhood centers,
    overnight camps and part-time schools.
    Scraped from https://www.jdata.com/tools/directory, Feb 2016
    """
    new_col_names = {'Address': 'Addr',
                     'Type of Organization': 'Type',
                     'Denominations': 'Denom'}
    df = (pd.read_json(fp)
          .rename(columns=new_col_names)
          .reset_index(drop=True)
          )

    df['Denom'] = (df['Denom'].fillna('None')
                   .replace('Pluralist or Transdenominational',
                            'Pluralist/Transdenom')
                   )
    df = manual_imputes(df)

    # reorder columns
    cols = ['Name', 'Addr', 'City', 'State', 'Zip', 'Country',
            'Phone', 'URL', 'Type', 'Denom']
    df = df[cols]

    return df

def merge_to_four_denoms(df, denom_col='Denom'):
    """Merges denomination categories for JData orgs."""
    df = df.copy()

    # Orthodox more common for Sephardic miscats
    df[denom_col] = df[denom_col].replace('Sephardic', 'Orthodox')

    # Traditional could be conservative but more often Orthodox
    # http://www.jewfaq.org/movement.htm
    # also single Traditional miscat is Orthodox
    df[denom_col] = df[denom_col].replace('Traditional', 'Orthodox')

    # Community schools tend to be multi-denominational
    # https://www.ou.org/jewish_action/11/2013/community-day-schools-option/
    # This is confirmed by Community miscats being most common for
    # 'Pluralist/Transdenom'

    df[denom_col] = df[denom_col].replace('Community', 'Pluralist/Transdenom')

    # non-mainstream Denom vals are generally interdenominational
    df[denom_col] = df[denom_col].replace(['Pluralist/Transdenom', 'None'], )

    df[denom_col] = df[denom_col].replace(
        ['Reconstructionist', 'Pluralist/Transdenom',
         'Humanistic', 'Secular', 'None'],
        'Other')

    return df

def get_miscats(data, cat_col, pretty_print=False):
    """
    Assess instances that may be mis-categorized by looking at
    whether an instance's category name appears elsewhere in its
    data.

    Parameters
    ----------
    data : pandas.DataFrame
    cat : str, category
    pretty_print : bool
        Print county summary of output

    Returns
    -------
    miscat : dict
        Mapping category name to pandas.DataFrame

    Example
    -------
    get_miscats(data, 'Denom')
    """
    miscats = {}
    for cat in data[cat_col].unique():

        contains_cat = data.apply(lambda x: x.str.contains(cat, case=False))
        is_not_cat = data[cat_col]!=cat
        miscat = data[contains_cat.any(1) & is_not_cat]

        n = len(miscat)
        if n==0:  # no miscategorizations
            if pretty_print:
                print('{}: {}'.format(cat, n))
            continue

        miscats[cat] = miscat

        if pretty_print:
            print('{}: {}'.format(cat, n))
            for type_, rows in miscat.groupby(cat_col):
                print('\t{}: {}'.format(type_, len(rows)))

    return miscats

def extract_clean_jdata(df):

    # Extract relevant
    df = df[(df.Country!='CA') &  # day camps have no denom data!
            (~df.Type.isin(['Overnight camp', 'Day camp']))]

    # Clean duplicates
    df = df[~df.duplicated(['Addr', 'Zip', 'Type', 'Denom'], keep='first')]

    # Partial impute of Denom using Denom as potential keyword
    # in other features.
    # Rest of imputation done at the county count level
    miscats_dict = get_miscats(df, 'Denom', pretty_print=False)

    # Ignore community when found in Addr
    miscats_dict['Community'] = (
        miscats_dict['Community']
        .loc[~miscats_dict['Community']
             .Addr.str.contains('Community', case=False)])

    # Finally impute missing
    for type_, data in miscats_dict.items():
        missing = data[data.Denom=='None'].index
        df.loc[missing, 'Denom'] = type_

    return df
