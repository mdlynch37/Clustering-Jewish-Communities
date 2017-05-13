#!/usr/bin/env python

import logging
import re
import time

import requests
from lxml import html
from pandas import DataFrame


class JDataDirScraper():
    """Scrapes directory of Jewish educational organization from JData.

    This starts with the directory search landing page to determine
    which categories are available as checkboxes in search fields.
    This category detail is not available through the listing pages,
    rather they must be assigned by iterating through all possible
    combinations.

    Organizations missing any categorical value are therefore only
    available through searches of records matching all categories.

    Parameters
    ----------
    base_url : str
        URL for directory search start page

    Attributes
    ----------
    n : int
        Number of records, retrieved from all records query.
    categories : dict
        dict of fields to their respective categories. Each category
        is a dict that maps to their corresponding query data
        used to access a category's subdirectory of organizations.

    Constants
    ---------
    BASE_URL : str
        Default, functions as both URL for directory search form
        (start page) and base URL for different search query URLS.
    OUT_FILEPATH : str
        Default path for saving extracted data.
    CAT_FIELDS : 2-tuple of str
        Search fields that represent organization categories.
        Length is hard-wired, would miss 3rd etc. category
        fields.
    MAX_REQUESTS : int
        Number of times to try
    """

    logging.basicConfig(filename='jdata_scraper.log', filemode='w',
                        format='%(levelname)s:%(message)s',
                        level=logging.INFO)

    BASE_URL = 'https://www.jdata.com/tools/directory/'
    OUT_FILEPATH = './jdata_directory.json'
    CAT_FIELDS = ('Type of Organization', 'Denominations')
    MAX_REQUESTS = 10

    def __init__(self, base_url=BASE_URL):
        self._logger = logging.getLogger(__name__)
        self.base_url = base_url
        self.field1 = self.CAT_FIELDS[0]
        self.field2 = self.CAT_FIELDS[1]
        self.n = 0
        self.df = None

        request = requests.get(self.base_url)
        self._base_tree = html.fromstring(request.content)
        for _ in range(self.MAX_REQUESTS):
            try:
                request = requests.get(base_url)
                self._base_tree = html.fromstring(request.content)
            except requests.exceptions.SSLError as e:
                continue
            break
        else:
            self._logger.error('For {}: {}'.format(self.url, e))

        self.categories = {}
        for field in (self.field1, self.field2):
            cats = self._extract_categories(field)
            self.categories[field] = cats

    def _extract_categories(self, field):
        """Extract category names for a field.

        Looks at base URL for directory search start page.

        Parameters
        ----------
        field : str
            Field name from which to extract categories,
            e.g. Denominations

        Returns
        -------
        cats : list of dicts
            2-tuple of categorical fields mapping to dict of category
            names and data for URL-based query of directory subset.
        """
        cats = {}
        xp = '//fieldset[child::legend[text()="{}"]]/ul/li'.format(field)

        for li in self._base_tree.xpath(xp):
            cat_name = li.findtext('label')

            # Field and value used to query subsets of organizations of a
            # particular category (appended to base URL).
            in_name = li.find('input').attrib['name']
            in_val = li.find('input').attrib['value'].replace(' ', '+')

            cats.update({
                cat_name: {'in_name': in_name, 'in_val': in_val}
                })

        return cats

    def extract_data(self):
        records = self._extract_all_combos()
        self.df = self._merge_duplicates(records)

        return self.df

    def to_json(self, filepath=OUT_FILEPATH):
        self.df.to_json(filepath)

    def _extract_all_combos(self):
        """Extracts records from all possible queries.

        Using combinations with 'All' checked is only way to
        ensure records with missing category records are picked up.
        This produces a sizable portion of records duplicated
        across categories.

        Returns
        -------
        records : list of dicts
            Each element is a record, with field to value dict mapping.
        """
        records =[]
        for cat1, cat1_in in self.categories[self.field1].items():
            for cat2, cat2_in in self.categories[self.field2].items():

                query_url = (
                    'results?fKeyword=&{n_0}={v_0}&{n_1}={v_1}'
                    .format(n_0=cat1_in['in_name'], v_0=cat1_in['in_val'],
                            n_1=cat2_in['in_name'], v_1=cat2_in['in_val'])
                    )
                query_url = ''.join([self.base_url, query_url])
                self._logger.info(
                    'Extracting from: {}\nType: {}\nDenom: {} ...'
                    .format(query_url, cat2, cat1))
                page = JDataPage(query_url)

                if not page.n:
                    self._logger.info('No records found for this subset.')
                else:
                    if cat1 == cat2 == 'All':
                        self.n = page.n
                    queried_data = page.extract_page()
                    for record in queried_data:
                        record.update({self.field1: cat1, self.field2: cat2})
                    records += queried_data
                time.sleep(1)

        return records

    def _merge_duplicates(self, records):
        """Eliminates duplicated records from 'All' queries.

        Duplicates are records categorized with both queried field data.
        (Records missing either or both of these fields are only accessible
        through 'All' queries.)

        Parameters
        ----------
        records : list of dicts
            Each element is a record, with field to value dict mapping.

        Returns
        -------
        df : Dataframe
            Records of organizations
        """
        ordered_cols = [
            'Name', 'Address', 'City', 'State', 'Zip', 'Country',
            'Phone', 'URL', self.field1, self.field2
            ]
        df = (DataFrame(records)
              .reindex(columns=ordered_cols)
              .replace({self.field1: {'All': None},
                        self.field2: {'All': None}})
              )
        # Extract single instance of any full duplicates, i.e. those that
        # exist in the source database regardless of extraction method.
        cols = df.columns.tolist()
        exact_dupes = df[df.duplicated()].drop_duplicates(subset=cols[:-2])

        # DataFrame is sorted with stable sorting algorithm mergesort.
        # This ensures that as rows are sorted on each column moving
        # from right to left, rows with most non-NaN vals for category
        # fields (last two cols) are at the top of groups of duplicates.
        # Top most row is then kept in dupe drop.
        df = df.sort_values(cols, kind='mergesort')
        df = df.drop_duplicates(subset=cols[:-2])

        total_extracted = len(df) + len(exact_dupes)
        if total_extracted == self.n:
            final_report = ('Successfully extracted all {} records!'
                            .format(self.n)
                            )
            self._logger.info(final_report)
        else:
            final_report = ('\nMISSING {} RECORDS:'
                            '\nTotal header for "All" records: {}'
                            '\nTotal extracted: {}'
                            .format(self.n-total_extracted,
                                    total_extracted, self.n)
                            )
            self._logger.error(final_report)
        print(final_report)

        return df

class JDataPage():
    """Scrapes organization data in a subset of JData directory.

    Parameters
    ----------
    url : str
        URL is for a single page containing organization records.

    Attributes
    ----------
    records : list of dicts
        Each element is a record, with field to data point dict mapping.
    n : int
        Number of records taken from page header.

    Constants
    ---------
    MAX_REQUESTS : int
        Maximum number of requests to attempt
    """

    MAX_REQUESTS = 10

    def __init__(self, url):
        self._logger = logging.getLogger(__name__)
        self.records = []
        self.url = url

        for _ in range(self.MAX_REQUESTS):
            try:
                request = requests.get(self.url)
                self._tree = html.fromstring(request.content)
            except requests.exceptions.SSLError as e:
                continue
            break
        else:
            self._logger.error('For {}: {}'.format(self.url, e))

        self.n = int(self._tree.xpath(
            '//*[@id="pageContentWrapper"]//strong')[0].text
            )

    def extract_page(self):
        """Parses list of organizations and their information.

        Returns
        -------
        self.records : list of dicts
            Each element is a record, with field to value dict mapping.
        """
        titles = self._tree.xpath(
            '//*[@id="pageContentWrapper"]//h7'
            )

        # Allows for missing fields
        city_state_re = re.compile(r'''
                (?P<City>    [^,]*?)?,[ ]
                (?P<State>   [A-Z]{2})? ([ ]
                (?P<Zip>     (\d{5}(-\d{4})?)|                # US zip
                             ([A-Z0-9]{3}[ ][A-Z0-9]{3}))$)?  # Canadian zip
                ''', re.VERBOSE)

        phone_re = re.compile(r'(?P<Phone>(\d{3}-)?\d{3}-\d{4})')

        for title in titles:
            name = title.text
            record = dict(Name=name)

            # details are in paragraph element that follows
            details = title.getnext()
            if details.tag!='p':
                self.records.append(record)
                continue

            # Starts with top line of address, will update Address later
            # with 2nd line of address or any other text not matched
            # to city/state/zip and phone regular expressions above.
            addr = details.text.strip() if details.text is not None else ''
            record.update(dict(Address=addr))

            link = details.find('a')
            if link is not None:
                record['URL'] = link.get('href')

            for br in details.findall('br'):
                line = br.tail.strip()
                if not line:
                    continue
                ph = phone_re.search(line)
                if ph:
                    record.update(ph.groupdict())
                    continue
                city_state = city_state_re.search(line)
                if city_state:
                    record.update(city_state.groupdict())
                    continue

                addr = '; '.join([addr, line])
                record.update(dict(Address=addr))
                self._logger.warn(u'2-line address or unusual format'
                                  u'\n\tName: {}\n\tInfo: {}\n\tAddress: {}'
                                  .format(name, html.tostring(details), addr))

            zip_ = record.get('Zip')
            if zip_:
                record['Country'] = ('US' if zip_.replace('-', '').isdigit()
                                     else 'CA')

            self.records.append(record)

        if self.n==len(self.records):
            self._logger.info('Successfully extracted all {} records!'
                              .format(self.n))

        return self.records


def main():
    j = JDataDirScraper()
    j.extract_data()
    j.to_json()


if __name__ == '__main__':
    main()
