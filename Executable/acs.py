import pandas as pd
from utilities import split_state, state_to_abbr, code_to_str

def read_acs_israeli_ancestry(tot_fp, single_fp, multiple_fp,
                              drop_counties=False):
    """Compiles county data from the ACS for Jewish ancestry.

    Tables can be retrieved from the American FactFinder at:
    https://factfinder.census.gov
    Note: Geography must be specified as county and then
    table must be transposed.

    Latest/best version of tables: 2015 ACS 5-year estimates

    Parameters
    ----------
    tot_fp : str
        Filepath to table B01003 (TOTAL POPULATION)
    single_fp : str
        Filepath to table B04005 (PEOPLE REPORTING MULTIPLE ANCESTRY)
    multiple_fp : str
        Filepath to table B04005 (PEOPLE REPORTING MULTIPLE ANCESTRY)
    drop_counties : bool
        If True, drop counties that do not have Jewish ethnicity data

    Returns
    -------
    df : pandas.DataFrame
    """
    df = pd.read_csv(tot_fp, skiprows=1, encoding='latin',
                     usecols=['Id2', 'Geography', 'Estimate; Total'])

    # Combine total populations of counties with ancestry data
    for fp in (single_fp, multiple_fp):
        tbl = pd.read_csv(fp, skiprows=1, encoding='latin',
                          usecols=['Id2', 'Estimate; Total: - Israeli'])
        df = df.merge(tbl, how='left', on='Id2')

    # rename columns
    df.columns = ['FIPS', 'County', 'Total_Pop',
                  'Only_Israeli_No', 'Part_Israeli_No']

    df.FIPS = code_to_str(df.FIPS, 5)
    df = (df.set_index('FIPS')
          .pipe(split_state, 'County')
          )
    df.State = df.State.pipe(state_to_abbr)

    # Create percentage and totals columns
    df = (df.assign(Only_Israeli_Pc=df.Only_Israeli_No / df.Total_Pop * 100)
            .assign(Part_Israeli_Pc=df.Part_Israeli_No / df.Total_Pop * 100)
            .assign(Tot_Israeli_No=df.Only_Israeli_No + df.Part_Israeli_No)
          )
    df = df.assign(Tot_Israeli_Pc=df.Part_Israeli_Pc + df.Only_Israeli_Pc)

    # reorder columns
    columns = ['County', 'State', 'Total_Pop',
               'Only_Israeli_No', 'Only_Israeli_Pc',
               'Part_Israeli_No', 'Part_Israeli_Pc',
               'Tot_Israeli_No', 'Tot_Israeli_Pc']
    df = df[columns]

    if drop_counties:
        df = df[(df.iloc[:, 4:]!=0).all(1)]

    return df
