# coding=<utf-8>

from collections import OrderedDict
import numpy as np
import pandas as pd
from utilities import state_to_abbr

PSS_TABLE_COLS = OrderedDict([
    ('School ID - NCES Assigned [Private School] Latest available year', 'ID'),
    ('Private School Name [Private School] 2011-12', 'Name'),
    ('Private School Name', 'Name_shorter'),  # some names cut off, use other
    ('State Name [Private School] Latest available year', 'State'),
    ('State Name [Private School] 2011-12', 'State2'),
    ('State Abbr [Private School] Latest available year', 'State3'),
    ('ANSI/FIPS State Code [Private School] Latest available year',
        'FIPS_State'),
    ('County Name [Private School] 2011-12', 'County'),
    ('ANSI/FIPS County Code [Private School] 2011-12', 'FIPS'),
    ('City [Private School] 2011-12', 'City'),
    ('Mailing Address [Private School] 2011-12', 'Addr'),
    ('Phone Number [Private School] 2011-12', 'Phone'),
    ('ZIP [Private School] 2011-12', 'Zip'),
    ('ZIP4 [Private School] 2011-12', 'Zip4'),
    ('ZIP + 4 [Private School] 2011-12', 'Zip_Full'),
    ('Library or Library Media Center [Private School] 2011-12', 'Library'),
    ('School Community Type [Private School] 2011-12', 'Community'),
    ('Urban-centric Locale [Private School] 2011-12', 'Locale'),
    ('Coeducational [Private School] 2011-12', 'Coed'),
    ('School Type [Private School] 2011-12', 'Type'),
    ('School Level [Private School] 2011-12', 'Level'),
    ("School's Religious Affiliation or Orientation [Private School] 2011-12",
        'Orientation'),
    ('Religious Orientation [Private School] 2011-12', 'Religion'),
    ('Days per School Year [Private School] 2011-12', 'Days'),
    ('Length of School Day in Total Hours (Including reported minutes) '
        '[Private School] 2011-12', 'Hours'),
    ('Pupil/Teacher Ratio [Private School] 2011-12', 'Student_Teach_Ratio'),
    ('Full-Time Equivalent (FTE) Teachers [Private School] 2011-12',
        'Teachers_FTE'),
    ('Total Students (Ungraded & PK-12) [Private School] 2011-12',
        'Total_Students'),
    ('Total Students (Ungraded & K-12) [Private School] 2011-12',
        'Total_Students_Excl_PK'),
    ('Prekindergarten and Kindergarten Students [Private School] 2011-12',
        'PK-K'),
    ('Grades 1-8 Students [Private School] 2011-12', 'Gr1-8'),
    ('Grades 9-12 Students [Private School] 2011-12', 'Gr9-12'),
    ('Ungraded Students [Private School] 2011-12', 'Ungraded'),
    ('Lowest Grade Taught [Private School] 2011-12', 'Lowest_Gr'),
    ('Highest Grade Taught [Private School] 2011-12', 'Highest_Gr'),
    ('Prekindergarten Students [Private School] 2011-12', 'PK_Gr'),
    ('Kindergarten Students [Private School] 2011-12', 'K_Gr'),
    ('Grade 1 Students [Private School] 2011-12', '1_Gr'),
    ('Grade 2 Students [Private School] 2011-12', '2_Gr'),
    ('Grade 3 Students [Private School] 2011-12', '3_Gr'),
    ('Grade 4 Students [Private School] 2011-12', '4_Gr'),
    ('Grade 5 Students [Private School] 2011-12', '5_Gr'),
    ('Grade 6 Students [Private School] 2011-12', '6_Gr'),
    ('Grade 7 Students [Private School] 2011-12', '7_Gr'),
    ('Grade 8 Students [Private School] 2011-12', '8_Gr'),
    ('Grade 9 Students [Private School] 2011-12', '9_Gr'),
    ('Grade 10 Students [Private School] 2011-12', '10_Gr'),
    ('Grade 11 Students [Private School] 2011-12', '11_Gr'),
    ('Grade 12 Students [Private School] 2011-12', '12_Gr'),
    ('American Indian/Alaska Native Students [Private School] 2011-12',
        'Amer_Ind_No'),
    ('Percentage of American Indian/Alaska Native Students '
        '[Private School] 2011-12', 'Amer_Ind_Pc'),
    ('Asian/Pacific Islander Students [Private School] 2011-12', 'Asian_No'),
    ('Percentage of Asian/Pacific Islander Students [Private School] 2011-12',
        'Asian_Pc'),
    ('Hispanic Students [Private School] 2011-12', 'Hispanic_No'),
    ('Percentage of Hispanic Students [Private School] 2011-12',
        'Hispanic_Pc'),
    ('Black Students [Private School] 2011-12', 'Black_No'),
    ('Percentage of Black Students [Private School] 2011-12', 'Black_Pc'),
    ('White Students [Private School] 2011-12', 'White_No'),
    ('Percentage of White Students [Private School] 2011-12', 'White_Pc'),
    ('Hawaiian Nat./Pacific Isl. Students [Private School] 2011-12',
        'Hawaiian_No'),
    ('Percentage of Hawaiian Nat./Pacific Isl. Students '
        '[Private School] 2011-12', 'Hawaiian_Pc'),
    ('Two or More Races Students [Private School] 2011-12', 'Multi_No'),
    ('Percentage of Two or More Races Students [Private School] 2011-12',
        'Multi_Pc')
    ])

def read_pss_table(fp, columns=PSS_TABLE_COLS):
    """Reads 2012-11 Private School Survey retrieved from
    online table generator.

    Table contains all available fields for all states. To retrieve, use
    Table ID 54242 at https://nces.ed.gov/ccd/elsi/tableGenerator.aspx

    Parameters
    ----------
    fp : str
        filepath to .csv file
    columns : OrderedDict, dict
        Original labels mapped to new labels, with option to
        specify order.

    Returns
    -------
    df, header, footer : tuple
        df : pandas.DataFrame str, str
        header : str
        footer : str
    """
    def clean_text(df):
        """Strips extra whitespace and annotation keys."""

        df = (df.replace(r'="(\d+([.]\d+)?)"', r'\1', regex=True)
              .replace(r'[†‡–]', np.nan, regex=True)  # for float conversion
              )
        return df

    n_header_rows, n_footer_rows = 5, 4
    with open(fp) as f:
        header = '\n'.join(next(f).strip() for _ in range(n_header_rows))

    df = pd.read_csv(fp, skiprows=n_header_rows, dtype=str)

    footer = '\n'.join(
        line.strip() for _, line in df.iloc[-n_footer_rows:, 0].iteritems()
        )
    df = df.iloc[:-(n_footer_rows+1), :]  # drops totals row too

    df = (df.rename(columns=columns)
          .apply(lambda x: x.str.strip())
          .pipe(clean_text)
          .set_index('ID')
          )
    df['State'] = state_to_abbr(df.loc[:, 'State'])

    float_cols = (
        ['Days', 'Hours'] + df.loc[:, 'Total_Students':].columns.tolist()
        )
    df[float_cols] = df[float_cols].astype(float)

    return df, header, footer

def read_pss_raw_dict(fp):
    """Reads data dict for raw 2011-12 Private School
    Universe Survey data.
    """
    NEW_COL_NAMES = {
        'POSITION'       : 'Pos',
        'NAME'           : 'Name',
        'TYPE'           : 'Var_Type',
        'LENGTH'         : 'Len',
        'QUESTION NUMBER': 'Code',
        'LABEL'          : 'Label'
        }
    QUESTION_CODES = {
        'Weighting Variable': 'weight',
        'Created Variable'  : 'created',
        'Frame Variable'    : 'frame',
        'Imputation Flag'   : 'impute'
        }
    df = (pd.read_csv(fp)
          .rename(columns=NEW_COL_NAMES)
          .replace(dict(Code=QUESTION_CODES))
          .set_index('Name')
          )
    df['QNum'] = df.Code.str.extract(r'^Q(\d+)', expand=False)

    return df

def read_pss_raw(fp):
    """Reads raw 2011-12 Private School Universe Survey data."""

    df = (pd.read_csv(fp, sep='\t', encoding='latin-1')
          .rename(columns=lambda x: x.strip().upper())
          .rename(columns=dict(PPIN='ID'))
          .set_index('ID')
          )
    return df

if __name__=='__main__':
    PSS_FP = '../Data/Schools/ELSI_csv_export_6362018465286175957399.csv'
    pss_df = read_pss_table(PSS_FP)
