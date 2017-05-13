import pandas as pd
from utilities import split_state, state_to_abbr, code_to_str

def read_ajpp_geo(fp):
    """Reads AJPP Geography lookup table.

    Coupled with the American Jewish Population Project data.

    FIPS column:
        Federal Information Processing Standard (FIPS) county codes.
    Region_States column:
        For regions that span multiple states, values
        are multiple states separated by dashes

    NOTE: Manually changed states for the following regions to
    account for missing states in some multi-state regions:
        - Outlying Region, AZ-CA
        - Washington DC & Northwest Suburbs, MD-DC
        - St. Paul & Surrounding Area, MN-WI
        - Lincoln & Omaha Area, NE-IA
        - Cincinnati Area, OH-KY
        - Portland Area, OR-WA

    Parameters
    ----------
    fp : str
        Filepath to .xlsx spreadsheet

    Returns
    -------
    df : pandas.DataFrame
    """
    df = (pd.read_excel(fp, parse_cols=[1, 2, 3], skiprows=2, skip_footer=4,
                        names=['Region', 'County', 'FIPS'])
          .fillna(method='ffill')
          )
    df.FIPS = code_to_str(df.FIPS, 5)
    df = df.set_index('FIPS')

    # df = df.apply(lambda x: x.str.strip())
    df = split_state(df, 'County', 'State')

    # for regions that span states
    df = split_state(df, 'Region', suffix='States')

    # fix typos
    df.Region = df.Region.str.replace(
        'Washington DC & Northwest Suburbs, MD',
        'Washington DC & Northwest Suburbs, DC'
        )
    return df

def read_ajpp_pop(fp, geo_fp):
    """Reads 2015 region-level Jewish population data.

    Data compiled by Steinhardt Social Research Institute's
    American Jewish Population Project.
    http://bir.brandeis.edu/handle/10192/25470

    Region_States column:
        For regions that span multiple states, values
        are multiple states separated by dashes. Regions from population
        data file only have primary state for multi-state regions.
        See County Group Definitions .xlsx spreadsheet for county
        breakdown of regions.
    Total_Adults columns:
        U.S. Census Bureau, Current Population Estimates 2015 adusted to
        household (w/in region) from 2010 Census.

    NOTE: Manually added superscript 3 to 'Mobile-Pensacola Region' and
    'Toledo Area', fixing an error given that it is a mutli-state region.

    Parameters
    ----------
    fp : str
        Filepath to .xlsx spreadsheet with population data
    geo_fp : str
        Filepath to .xlsx of geography definitions

    Returns
    -------
    df : pandas.DataFrame
    """
    def parse_state_headers(df):
        """Creates state column for regions from header cols."""

        # state header rows have no other values
        states = df[df.drop('Region', axis=1).isnull().all(1)].Region

        n_states = len(states)
        state_pos = states.index.tolist()

        # shift state header rows into Primary_State col and fill
        # for the resp. state's regions.
        for cnt, (idx, state) in enumerate(states.iteritems(), 1):
            start = idx
            stop = state_pos[cnt] if cnt < n_states else None
            df.loc[start:stop, 'Primary_State'] = state

        df = df.drop(states.index)  # drop state header rows
        df['Primary_State'] = state_to_abbr(df['Primary_State'])

        return df

    def primary_to_all_states(df, geo_fp):
        """
        Replaces single primary state to mutliple states for
        multi-state regions (provided only in geo file), renaming
        Primary_State col to Region_States
        """

        df = df.rename(columns={'Primary_State': 'Region_States'})
        geo_df = read_ajpp_geo(geo_fp)

        # for each region that spans states, indicated by superscript 3
        is_multistate = df.Region.str.endswith(u'\xb3')
        df.Region = df.Region.str.strip(u'\xb3')
        for idx, region in df.loc[is_multistate, 'Region'].iteritems():
            df.loc[idx, 'Region_States'] = (geo_df[geo_df.Region==region]
                                            .ix[0, 'Region_States'])

        return df

    def clean_region(df):
        """Strips extra whitespace and annotation keys."""

        df['Region'] = (df.Region.replace(r'\s+,\s+', ', ', regex=True)
                        .str.strip()
                        )
        return df

    def fix_typos(df):
        df['Region'] = df.Region .str.replace(
            'Albuquerque, Sante Fe & Durango Regions',
            'Albuquerque, Santa Fe & Durango Regions'
            )
        return df

    df = pd.read_excel(
        fp, parse_cols=[0, 1, 6], skiprows=7, skip_footer=9,
        names=['Region', 'Total_Adults', 'Jewish_By_Rel']
        )
    df = (df.pipe(clean_region)
          .pipe(fix_typos)
          .pipe(parse_state_headers)
          .pipe(primary_to_all_states, geo_fp)
          .reset_index(drop=True)
          )
    int_cols = ['Total_Adults', 'Jewish_By_Rel']
    df[int_cols] = df[int_cols].astype(int)

    # reorder columns
    columns = ['Region_States', 'Region', 'Total_Adults', 'Jewish_By_Rel']
    df = df.reindex(columns=columns)

    return df


if __name__=='__main__':
    read_ajpp_pop('../Data/Demography/AJPP_County2015.xlsx')
