import itertools as it

from acs import ACSCountyReader

def test_acsreader(data_dir='./'):
    """
    Runs through all combinations of valid parameters
    and passes if no exceptions are raised.
    """
    POP_FP               = ''.join([data_dir, 'ACS_15_5YR_B01003/',
                                    'ACS_15_5YR_B01003_with_ann.csv'])
    ACS_FOREIGN_BIRTH_FP = ''.join([data_dir, 'ACS_15_5YR_B05006/',
                                    'ACS_15_5YR_B05006_with_ann.csv'])
    SNGL_ANCE_FP         = ''.join([data_dir, 'ACS_15_5YR_B04004/',
                                    'ACS_15_5YR_B04004_with_ann.csv'])
    MULT_ANCE_FP         = ''.join([data_dir, 'ACS_15_5YR_B04005/',
                                    'ACS_15_5YR_B04005_with_ann.csv'])
    ALL_ANCE_FP          = ''.join([data_dir, 'ACS_15_5YR_B04006/',
                                    'ACS_15_5YR_B04006_with_ann.csv'])

    fps = [POP_FP, ACS_FOREIGN_BIRTH_FP, SNGL_ANCE_FP,
           MULT_ANCE_FP, ALL_ANCE_FP]

    kws = [False, 'test_kw']
    names = [None, True, 'test_names']
    drop_tot = [False, True]
    drop_moe = [False, True]

    for fp in fps:
        acs = ACSCountyReader(fp)
        for args in it.product(kws, names, drop_tot, drop_moe):
            acs.read_counties(*args)

    print('Basic tests passed: no exceptions raised')


if __name__=='__main__':

    DATA_DIR    = '../Data/'
    DEM_DIR = ''.join([DATA_DIR, 'Demography/'])
    test_acsreader(DEM_DIR)
