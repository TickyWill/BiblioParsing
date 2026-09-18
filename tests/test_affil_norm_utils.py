"""

"""

import bpfuncts.affil_norm_utils as affil_norm_utils


def test__build_words_set_vanilla():
    raw_aff = "Université des sciences et de la technologie Houari Boumédiène"
    assert affil_norm_utils._build_words_set(raw_aff, verbose=True) == (
        {'technologie', 'des', 'houari', 'university', 'boumediene', 'sciences'}, {}
    )

def test__build_words_set_missing_space_acronyms():
    raw_aff = "UMR 7190"
    assert affil_norm_utils._build_words_set(raw_aff, verbose=True) == ({'umr', '7190'}, {'umr7190'})


def test__build_words_sets_list():
    raw_aff_list = ['AIM', 'UMR 7158', 'Astrophysique interprétation modélisation']
    assert affil_norm_utils._build_words_sets_list(raw_aff_list, True) == [
        {'aim'},
        {'umr', '7158'},
        {'umr7158'},
        {'modelisation', 'interpretation', 'astrophysique'}
    ]


def test_build_norm_raw_affils_dict_vanilla():
    result = affil_norm_utils.build_norm_raw_affils_dict(verbose=True)
    assert isinstance(result, dict)
    assert len(result) > 0


def test_build_norm_raw_affils_dict_small_data_file():
    result = affil_norm_utils.build_norm_raw_affils_dict('./bpfuncts/RefFiles/Country_affiliations_small.xlsx')
    assert result == {
        'Algeria':
            {'EMP Gsch': [
                {'emp'}, {'polytech', 'mil', 'ecole'},
                {'polytechnique', 'ecole', 'militaire'}
            ], 'HCR Nro': [
                {'hcr'}, {'haut', 'commissariat', 'recherche'}
            ], 'HCR-CRTI Inst': [
                {'crti'}, {'rcit'}, {'industrial', 'center', 'research', 'technologies'}
            ], 'MKUB Univ': [
                {'university', 'khider', 'mohamed'}, {'university', 'biskra'}
            ], 'OEB Univ': [
                {'el', 'university', 'bouaghi', 'oum'}, {'oeb', 'university'}
            ], 'Oran1 Univ': [
                {'university', 'oran'}, {'university', 'oran1'}, {'oran', '1', 'ahmed', 'bella', 'university', 'ben'}
            ], 'Oran2 Univ': [
                {'university', 'oran2'}, {'oran', 'mohamed', 'ahmed', 'university', '2', 'ben'}
            ], 'UDL-SBA Univ': [
                {'liabbes', 'djilalli', 'university'}
            ], 'UC# Univ': [
                {'constantine', 'university'},
                {'university', 'constantine1'},
                {'constantine', 'freres', '1', 'university', 'mentouri'},
                {'constantine3', 'university'}
            ], 'UC1-CHEMS Lab': [
                {'chems'}, {'unite', 'environm', 'rech', 'chim', 'struct', 'mol'}
            ], 'UMAB Univ': [
                {'mostaganem', 'university'},
                {'mostaganem', 'abdelhamid', 'badis', 'ibn', 'university'},
                {'technology', 'sciences', 'faculty'}
            ], 'UMAB-FSEI Fac': [
                {'sci', 'comp', 'exact', 'faculty'}
            ], 'UMMTO Univ': [
                {'ummto'},
                {'university', 'mammeri', 'mouloud'},
                {'ouzou', 'tizi', 'university'}
            ], 'UOran1-LCM Lab': [
                {'lcm'}, {'materials', 'chemistry', 'laboratory'}, {'materiaux', 'chimie', 'laboratory', 'des'}
            ], 'USTHB Univ': [
                {'usthb'},
                {'technologie', 'sciences', 'boumediene', 'university', 'houari', 'des'},
                {'process', 'mechanical', 'engineering', 'faculty'},
                {'proc', 'mech', 'engn', 'faculty'},
                {'sciences', 'university', 'technlgiehuaribumediene', 'des'}
            ], 'USTHB-LAOS Lab': [
                {'synthesis', 'applied', 'organic', 'laboratory'}
            ], 'USTHB-LTPM Lab': [
                {'ltpmp'}, {'media', 'multiphase', 'porous', 'laboratory', 'flows'}
            ], 'UYF Univ': [
                {'uyf'},
                {'yahia', 'dcteur', 'university', 'medea', 'fares'},
                {'yahia', 'university', 'medea', 'fares', 'docteur'}
            ]}
    }


def test_read_affil_types():
    expected_output = {'Firm': 1, 'Chu': 2, 'Univ': 3, 'Nro': 4, 'Rto': 5,
                       'Iro': 6, 'Lab': 7, 'CNRS-Lab': 8, 'Univ-Lab': 9, 'Gsch': 10,
                       'Inst': 11, 'Jlab': 12, 'CEA-Div': 13, 'CEA-Inst': 14, 'Gov': 15,
                       'Agn': 16, 'Lsf': 17, 'Fac': 18, 'Sch': 19, 'Div': 20,
                       'Dept': 21, 'CEA-Dept': 22, 'LETI-Dept': 23, 'Bunit': 24, 'Unit': 25,
                       'Cti': 26, 'Ctr': 27, 'Serv': 28, 'CEA-Serv': 29, 'CEA-Lab': 30,
                       'LETI-Serv': 31, 'LETI-Lab': 32, 'Beamline': 33, 'Team': 34, 'Ic': 35,
                       'Irt': 36, 'Ite': 37, 'Labex': 38, 'Fed': 39, 'Ntwk': 40,
                       'Pole': 41, 'CNRS-Pltf': 42, 'Pltf': 43, 'Site': 44, 'Art': 45,
                       'Dsch': 46}

    assert affil_norm_utils.read_affil_types() == expected_output


def test_read_towns_per_country():
    result = affil_norm_utils.read_towns_per_country()
    assert isinstance(result, dict)
    assert len(result) > 0


def test_read_towns_per_country_small_data_file():
    input_file = "Country_towns_small.xlsx"
    expected_output = {
        'China': [
            'anhui', 'beijing', 'changchun', 'changsha', 'chengdu', 'dalian',
            'guangdong', 'guangzhou', 'hangzhou', 'hefei', 'hunan', 'jiangsu',
            'lanzhou', 'liaoning', 'nanjing', 'ningbo', 'qingdao', 'shaanxi',
            'shandong', 'shanghai', 'shenyang', 'shenzhen', 'suzhou', 'taiyuan',
            'urumqi', 'wuhan', 'xian', 'xi an'
        ]
    }

    assert affil_norm_utils.read_towns_per_country(input_file) == expected_output


def test__check_norm_raw_affils_dict():
    pass


def test_build_affils_useful_dicts():
    pass
