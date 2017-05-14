import pandas as pd
from utilities import split_state, state_to_abbr, code_to_str

class ACSCountyReader():
    """Reads American Community Survey county-level data.

    Tested with 2015 5-year estimate demographic data, but likely compatible
    with prior years, etc.

    Tables can be retrieved from the American FactFinder at:
    https://factfinder.census.gov

    Files used:
    - ACS_15_5YR_B01003_with_ann.csv (POPULATION)
    - ACS_15_5YR_B04004_with_ann.csv (PEOPLE REPORTING SINGLE ANCESTRY)
    - ACS_15_5YR_B04005_with_ann.csv (PEOPLE REPORTING MULTIPLE ANCESTRY)
    - ACS_15_5YR_B04006_with_ann.csv (PEOPLE REPORTING ANCESTRY)
    - ACS_15_5YR_B05006_with_ann.csv (PLACE OF BIRTH FOR THE FOREIGN-BORN
         POPULATION IN THE UNITED STATES)
    """


    # _OG indicates the column name in original table
    # These are followed by new column name in the assignment
    FIPS_COL_OG, FIPS_COL = 'Id2', 'FIPS'
    GEO_COL_OG,  GEO_COL  = 'Geography', 'County'

    # column prefixes used by ACS»
    EST_PRE,    MOE_PRE    = 'Estimate; ', 'Margin of Error; '
    # suffix for renamed columns
    EST_SUF,    MOE_SUF    = '', '_Moe'

    TOT_EST_OG, TOT_EST    = EST_PRE+'Total', 'Tot'+EST_SUF
    TOT_MOE_OG, TOT_MOE    = MOE_PRE+'Total', 'Tot'+MOE_SUF

    GEN_COLS_OG = [FIPS_COL_OG, GEO_COL_OG, TOT_EST_OG, TOT_MOE_OG]
    GEN_COLS    = [FIPS_COL, GEO_COL, TOT_EST, TOT_MOE]

    TO_DROP = ['Id']  # never necessary

    def __init__(self, fp='./'):
        """
        Initaliatizes reader with filepath to table.
        """

        self.fp = fp

    def read_counties(self, kw=None, name=None, total=True, moe=False):
        """Reads ACS county-level demographic data.

        Parameters
        ----------
        kw : str
            Search term to filter out irrelevant sub-populations
            e.g. For Israeli ancestry, kw would be 'Israeli'
        name : str or True
            If provided, name used for relevant columns from kw
            If True, use kw as name for those columns.
        total : bool
            If True, include total column, with its MOE if included,
            renamed as 'Tot' and 'Tot_Moe'
        moe : bool
            If True, include margin of error data

        Returns
        -------
        df : pandas.DataFrame
        """

        df = pd.read_csv(self.fp, skiprows=1, encoding='latin')
        df = df.drop(self.TO_DROP, axis=1)

        # inconsistency across tables with total ending in ':'
        df = df.rename(columns=lambda x: x.strip(':'))

        df = df.rename(columns=dict(zip(self.GEN_COLS_OG, self.GEN_COLS)))

        if kw:
            kw_cols = [x for x in df.columns if kw in x]

            df = df.select(lambda x: x in self.GEN_COLS or x in kw_cols)

            if name is not None:
                # use kw arg value as new col name
                name = kw if name is True else name

                # prevents multiple kw matches from having the
                # same name, i.e. ambiguous, but also allowing
                # for when kw not found in columns
                if len(kw_cols) > 2:
                    raise ValueError('Renaming only supported when '
                                     'keyword present in only one variable')
                new_kw_cols = []
                for kw_col in kw_cols:
                    if self.EST_PRE in kw_col:
                        new_kw_cols.append(name+self.EST_SUF)
                    elif self.MOE_PRE in kw_col:
                        new_kw_cols.append(name+self.MOE_SUF)
                    else:
                        new_kw_cols.append(kw_col)

                df = df.rename(columns=dict(zip(kw_cols, new_kw_cols)))

        if not moe:
            df = df.select(
                lambda x: not x.endswith(self.MOE_SUF), axis=1
                )
        if not total:
            df = df.select(
                lambda x: x not in [self.TOT_EST, self.TOT_MOE], axis=1
                )

        df[self.FIPS_COL] = code_to_str(df[self.FIPS_COL], 5)
        df = df.set_index(self.FIPS_COL)
        df = split_state(df, self.GEO_COL)
        df.State = state_to_abbr(df.State)

        return df
