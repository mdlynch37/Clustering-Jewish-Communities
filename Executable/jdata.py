from collections import OrderedDict

import pandas as pd


class JData:
    """JData Jewish educational organization directory.

    Directory of Jewish day camps, day schools, early childhood
    centers, overnight camps and part-time schools for various
    denominations and orientations.

    Scraped from https://www.jdata.com/tools/directory, Feb 2016.
    
    CONSTANTS
    ---------
    COL_NAMES : dict
        Use to rename only the long column names.
    DENOM_NAMES : OrderedDict
        Use to rename all denomination values, and for reference.
        OrderedDict used so consistent output for get_miscats()
    TYPE_NAMES : dict
        Use to rename all type values, and for reference.
    """
    COL_NAMES = {
        'Address': 'Addr',
        'Type of Organization': 'Type',
        'Denominations': 'Denom',
    }
    DENOM_NAMES = OrderedDict([
        ('Orthodox', 'Orth'),
        ('Conservative', 'Consv'),
        ('Reform', 'Ref'),
        ('Reconstructionist', 'Recon'),
        ('Community', 'Comm'),
        ('Humanistic', 'Hum'),
        ('Sephardic', 'Seph'),
        ('Other', 'Oth'),
        ('Secular', 'Sec'),
        ('Traditional', 'Trad'),
        ('Pluralist or Transdenominational', 'PlurTrans'),
    ])

    TYPE_NAMES = {
        'Day camp': 'DayCamp',
        'Day school': 'DaySch',
        'Early childhood center': 'EarlyChild',
        'Overnight camp': 'OverCamp',
        'Part-time school': 'PTSch'
    }


    def __init__(self):
        """Simple initialization."""
        self.fp = None
        self.orgs_df = None
        self.clean = None
        self.only_usa = None

    def read_orgs(self, fp, clean=False, only_usa=False):
        """Read directory data from json file,
        and set instance variable.

        Parameters
        ----------
        fp : str, filepath to json scraped data
        clean : bool, if True apply various cleaning methods.
        only_usa : bool, if True only return orgs in the USA

        Returns
        -------
        orgs_df : DataFrame of JData directory
        """
        self.fp = fp
        self.clean = clean
        self.only_usa = only_usa


        orgs_df = (pd.read_json(fp)
                   .rename(columns=self.COL_NAMES)
                   .reset_index(drop=True)
        )

        orgs_df = orgs_df.replace(
            dict(Type=self.TYPE_NAMES, Denom=self.DENOM_NAMES)
        )
        # reorder columns
        reordered = [
            'Name', 'Addr', 'City', 'State', 'Zip', 'Country',
            'Phone', 'URL', 'Type', 'Denom'
        ]
        orgs_df = orgs_df[reordered]

        if clean:
            orgs_df = self.clean_orgs(orgs_df)
        if only_usa:
            orgs_df = self.filter_usa(orgs_df)

        self.orgs_df = orgs_df
        return self.orgs_df

    def clean_orgs(self, orgs_df):
        """Clean, fix and filter out Canadian orgs."""
        orgs_df = (orgs_df.pipe(self._manual_imputes)
              .pipe(self._impute_denoms_with_miscats)
              .pipe(self._correct_seph_miscats)
        )
        orgs_df = orgs_df.drop_duplicates(
                    ['Addr', 'City', 'State', 'Zip', 'Type', 'Denom']
        )
        return orgs_df

    @staticmethod
    def filter_usa(orgs_df):
        return orgs_df.loc[orgs_df.Country != 'CA'].drop(['Country'], axis=1)

    @staticmethod
    def combine_similar_denoms(orgs_df):
        """Combine denomination categories for JData orgs."""
        # noisy, so few
        orgs_df.Denom = orgs_df.Denom.replace('Seph', 'Orth')

        # Traditional more often Orthodox than conservative
        # http://www.jewfaq.org/movement.htm
        orgs_df.Denom = orgs_df.Denom.replace('Trad', 'Orth')

        # Similar with very few entries
        orgs_df.Denom = orgs_df.Denom.replace('Hum', 'Sec')

        return orgs_df

    @staticmethod
    def combine_non_demons(orgs_df):
        """Combine non-denominational orgs together."""

        non_denoms = ['Comm', 'PlurTrans', 'Hum', 'Sec']
        orgs_df.Denom = orgs_df.Denom.replace(non_denoms, 'NonDenom')

        return orgs_df

    def _impute_denoms_with_miscats(self, orgs_df):
        """Impute missing denoms using clues from other features.

        This does not impute all missing denominations. Best left to
        other superclass JDataCounty that can proportionally distribute
        nans across other denoms in county.
        """
        miscats_dict = self.get_denom_miscats(orgs_df)

        for type_, data in miscats_dict.items():
            missing = data[data.Denom.isnull() | (data.Denom == 'Oth')].index
            orgs_df.loc[missing, 'Denom'] = type_

        return orgs_df

    def _correct_seph_miscats(self, orgs_df):
        """Correct any orgs with Sephardic in Name or URL."""

        miscats_dict = self.get_denom_miscats(orgs_df)

        orgs_df.loc[miscats_dict['Seph'].index, 'Denom'] = 'Seph'
        return orgs_df

    def get_denom_miscats(self, orgs_df, pretty_print=False):
        """Return organizations that may be mis-categorized.

        Whether its Denomination value appears in its other
        features is an approximation used for analysis and imputation.

        Parameters
        ----------
        orgs_df : pandas.DataFrame
        pretty_print : bool, default is True
            Print county summary of output

        Returns
        -------
        miscat : dict
            Mapping category value to pandas.DataFrame of orgs that
            may be mis-categorized with current value.
        """
        if pretty_print:
            print(
                '{:<23}{:<23}\n'.format('FOUND ELSEWHERE', 'ACTUAL CATEGORY'))

        miscats = {}
        for full_denom, denom in self.DENOM_NAMES.items():
            if full_denom == 'Other':
                continue  # too broad

            contains_cat = orgs_df[['Name', 'URL']].apply(
                lambda x: x.str.contains(full_denom, case=False)
            )
            miscat = orgs_df[
                contains_cat.any(1) & (orgs_df.Denom != denom)
                ]

            miscats[denom] = miscat
            if pretty_print:
                # total of found categories
                print('{:>3} {:<23}'.format(len(miscat), denom))
                # list of actual categories for that found one
                for bad_cat, rows in miscat.fillna('None').groupby('Denom'):
                    print('{}{:>3} {}'.format(' ' * 23, len(rows), bad_cat))
        return miscats

    @staticmethod
    def _manual_imputes(orgs_df):
        """Manually impute missing location values.

        Level of detail is necessary for analysis of US counties that
        have small Jewish populations.
        """
        try:
            orgs_df.loc[orgs_df.State.isin(['QC', 'ON']), 'Country'] = 'CA'

            orgs_df.loc[260, ['Zip', 'Country']] = (
                '35031', 'US')
            orgs_df.loc[347, ['City', 'State', 'Zip', 'Country']] = (
                'Fallsburg', 'NY', '12733', 'Country')
            orgs_df.loc[696, ['State', 'Zip', 'Country']] = (
                'PA', '15217', 'US')
            orgs_df.loc[860, ['City', 'Zip', 'Country']] = (
                'Kahului', '96732', 'US')
            orgs_df.loc[1418, ['City', 'State', 'Zip', 'Country']] = (
                'Mentor', 'OH', '44060', 'US')

            orgs_df = orgs_df.drop([386, 2084, 2142, 2143])
        except ValueError as e:
            raise ValueError(
                str(e) + ': ' + 'Must be before dropping Canadian orgs.')

        return orgs_df

if __name__ == '__main__':

    DATA_DIR = '../Data/'
    SCL_DIR = ''.join([DATA_DIR, 'Schools/'])
    JDATA_FP = ''.join([SCL_DIR, 'jdata_directory.json'])

    orgs_df = JData().read_orgs(JDATA_FP, clean=True)
