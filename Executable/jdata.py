import pandas as pd


class JData:
    def __init__(self):
        """JData Jewish educational organization directory.

        Directory of Jewish day camps, day schools, early childhood
        centers, overnight camps and part-time schools for various
        denominations and orientations.

        Scraped from https://www.jdata.com/tools/directory, Feb 2016.
        """
        self.fp = None
        self.orgs_df = None

    def read_orgs(self, fp, clean=False, combine_denoms=False):
        """Read directory data from json file.

        Parameters
        ----------
        fp : str, filepath to json scraped data
        clean : bool, if True apply various cleaning methods.
        combine_denoms : bool, if True combine similar denominations.
            Note: data must be clean pre-denom combine.

        """
        self.fp = fp

        new_col_names = {
            'Address': 'Addr',
            'Type of Organization': 'Type',
            'Denominations': 'Denom',
        }
        df = (pd.read_json(self.fp).rename(columns=new_col_names).reset_index(
            drop=True))

        new_cat_names = dict(
            Denom={
                'Orthodox': 'Orth',
                'Conservative': 'Consv',
                'Reform': 'Ref',
                'Reconstructionist': 'Recon',
                'Community': 'Comm',
                'Humanistic': 'Hum',
                'Sephardic': 'Seph',
                'Other': 'Oth',
                'Secular': 'Sec',
                'Traditional': 'Trad',
                'Pluralist or Transdenominational': 'PlurTrans',
            },
            Type={
                'Day camp': 'DayCamp',
                'Day school': 'DaySch',
                'Early childhood center': 'EarlyChild',
                'Overnight camp': 'OverCamp',
                'Part-time school': 'PTSch'
            })
        df = df.replace(new_cat_names)

        # reorder columns
        cols = [
            'Name', 'Addr', 'City', 'State', 'Zip', 'Country', 'Phone', 'URL',
            'Type', 'Denom'
        ]
        df = df[cols]

        if clean:
            df = self.clean_orgs(df)

        if combine_denoms:
            if not clean:
                raise ValueError('Data must be cleaned before denom combine.')
            df = self.combine_denoms(df)

        return df

    def clean_orgs(self, df):
        """Clean, fix and filter out Canadian orgs."""
        df = (df.pipe(self._manual_imputes)
              .pipe(self._impute_denoms_with_miscats).loc[df.Country != 'CA']
              .drop(['Country'], axis=1).drop_duplicates(
                  ['Addr', 'City', 'State', 'Zip', 'Type', 'Denom']))
        return df

    def combine_denoms(self, df):
        """Group similar/smaller denominations.

        Note: Only works with clean data, i.e. after clean_orgs()
        """
        return (df.pipe(self._combine_similar_denoms)
                .pipe(self._combine_non_demons))

    @staticmethod
    def _combine_similar_denoms(df):
        """Combine denomination categories for JData orgs."""
        # noisy, so few
        df.Denom = df.Denom.replace('Seph', 'Orth')

        # Traditional more often Orthodox than conservative
        # http://www.jewfaq.org/movement.htm
        df.Denom = df.Denom.replace('Trad', 'Orth')

        # Similar with very few entries
        df.Denom = df.Denom.replace('Hum', 'Sec')

        return df

    @staticmethod
    def _combine_non_demons(df):
        """Combine non-denominational orgs together."""

        non_denoms = ['Comm', 'PlurTrans', 'Oth', 'Hum', 'Sec']
        df.Denom = df.Denom.replace(non_denoms, 'NonDenom')

        return df

    def _impute_denoms_with_miscats(self, df):
        """Impute missing denoms using clues from other features.

        This does not impute all missing denominations. Best left to
        other superclass JDataCounty that can proportionally distribute
        nans across other denoms in county.
        """
        miscats_dict = self.get_miscats(df, pretty_print=False)

        # Filter out those that are flagged because community
        # keyword found in Address (not useful)
        miscats_dict['Comm'] = (
            miscats_dict['Comm'].loc[~miscats_dict['Comm']
                                     .Addr.str.contains('Comm', case=False)])
        for type_, data in miscats_dict.items():
            missing = data[data.Denom.isnull()].index
            df.loc[missing, 'Denom'] = type_

        return df

    @staticmethod
    def get_miscats(df, cat_col='Denom', pretty_print=True):
        """Return organizations that may be mis-categorized.

        Whether its category (Denom or Type) value appears in its other
        features is an approximation used for analysis and imputation.

        Parameters
        ----------
        df : pandas.DataFrame
        cat_col : str, 'Denom' or 'Type'
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
        for cat in df[cat_col].dropna().unique():
            contains_cat = df.apply(lambda x: x.str.contains(cat, case=False))
            miscat = df[contains_cat.any(1) & (df[cat_col] != cat)]
            miscats[cat] = miscat
            if pretty_print:
                # total of found categories
                print('{:>3} {:<23}'.format(len(miscat), cat))
                # list of actual categories for that found one
                for bad_cat, rows in miscat.fillna('None').groupby(cat_col):
                    print('{}{:>3} {}'.format(' ' * 23, len(rows), bad_cat))
        return miscats

    @staticmethod
    def _manual_imputes(df):
        """Manually impute missing location values.

        Level of detail is necessary for analysis of US counties that
        have small Jewish populations.
        """
        try:
            df.loc[df.State.isin(['QC', 'ON']), 'Country'] = 'CA'

            df.loc[260, ['Zip', 'Country']] = '35031', 'US'
            df.loc[347, ['City', 'State', 'Zip', 'Country']] = (
                'Fallsburg', 'NY', '12733', 'Country')
            df.loc[696, ['State', 'Zip', 'Country']] = 'PA', '15217', 'US'
            df.loc[860, ['City', 'Zip', 'Country']] = 'Kahului', '96732', 'US'
            df.loc[1418, ['City', 'State', 'Zip', 'Country']] = (
                'Mentor', 'OH', '44060', 'US')

            df = df.drop([386, 2084, 2142, 2143])
        except ValueError as e:
            raise ValueError(
                str(e) + ': ' + 'Must be before dropping Canadian orgs.')

        return df
