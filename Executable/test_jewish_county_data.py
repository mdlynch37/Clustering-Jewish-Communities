import unittest

import numpy as np

import jdata as jd
import jewish_county_data as jc
from hud_geo_conversions import read_zips_to_fips

class TestJewishCountyData(unittest.TestCase):

    TYPE_LBL='Type'
    DENOM_LBL='Denom'
    HOW_PARAMS = ['simple', 'type', 'denom', 'all', 'combined']
    DATA_DIR = '../Data/'
    SCL_DIR = ''.join([DATA_DIR, 'Schools/'])
    JDATA_FP = ''.join([SCL_DIR, 'jdata_directory.json'])

    def read_jdata(self):
        jdata = jd.read_jdata(self.JDATA_FP)
        jdata = jdata[jdata.Country!='CA']
        return jdata

    def test_aggr_jdata_to_zip(self):
        """Tests by cycling through each methods and comparing
        their appropriate transformations with the simplest method
        as the benchmark.
        """
        jdata = self.read_jdata()

        for i, how in enumerate(self.HOW_PARAMS):
            with self.subTest(i=i, how=how):
                jd_cnts = jc.aggr_jdata_to_zip(jdata, how=how,
                                               type_lbl=self.TYPE_LBL,
                                               denom_lbl=self.DENOM_LBL)

                if i==0:
                    bench = jd_cnts.Tot_Orgs
                    continue

                # compare sum of all organization between simple and variant
                jd_cnts_total = jd_cnts.sum(1)
                # correct for double count with dummies for two categories
                if how=='all':
                    jd_cnts_total //= 2  # for int type comparison
                self.assertTrue(bench.equals(jd_cnts_total))

    def test_jd_zip_cnts_to_county(self):

        jdata = self.read_jdata()
        zips_to_fips = read_zips_to_fips()

        for how in self.HOW_PARAMS:
            with self.subTest(how=how):
                jd_zip_cnts = jc.aggr_jdata_to_zip(jdata, how=how,
                                                   type_lbl=self.TYPE_LBL,
                                                   denom_lbl=self.DENOM_LBL)
                result = jc.jd_zip_cnts_to_county(jd_zip_cnts, zips_to_fips)
                res_total = result.sum().sum()  # scalar, num orgs

                # correct for double count with dummies for two categories
                if how=='all':
                    res_total /= 2

                self.assertTrue(np.isclose(res_total, len(jdata)))


if __name__ == '__main__':
    unittest.main()
