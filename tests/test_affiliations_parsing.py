"""

"""
import re
from collections import namedtuple

import bpfuncts.affiliations_parsing as affiliations_parsing


def test__search_dropping_bp_false():
    input_data = ["test1", "test2", "test3"]
    assert affiliations_parsing._search_dropping_bp(input_data) == [False]


def test__search_dropping_bp_true():
    input_data = ["bp123"]
    assert affiliations_parsing._search_dropping_bp(input_data, verbose=True) == [True]


def test__set_droping_zipcode_pattern_uk_case():
    input_data = "United Kingdom"
    assert affiliations_parsing._set_droping_zipcode_pattern(input_data) == (
        '^\\s?[a-z]{1,2}\\d{1,2}[a-z]{0,1}\\s?\\d{1,2}[a-z]{1,2}$',
        False
    )


def test__set_droping_zipcode_pattern_canada_case():
    input_data = "Canada"
    assert affiliations_parsing._set_droping_zipcode_pattern(input_data) == (
        '^\\s?[a-z]{2}$|^\\s?[a-z]{2}\\s[a-z0-9]{3,4}\\s[a-z0-9]{2,3}$',
        False
    )


def test__set_droping_zipcode_pattern_regular_case():
    input_data = "France"
    assert affiliations_parsing._set_droping_zipcode_pattern(input_data) == (
        '\\b(f|fr)[\\s-]?(\\d{5})\\b|\\b(f|fr)[\\s-]?(\\d{6})\\b',
        False
    )


def test__set_droping_zipcode_pattern_notfound_case():
    input_data = "Ha ha ha"
    assert affiliations_parsing._set_droping_zipcode_pattern(input_data) == ('', True)


def test__set_digits_keeping_prefix_regex():
    expected_result = re.compile(
        '\\bea[-]?\\d{4}\\b|\\bfr[-]?\\d{4}\\b|\\bu[-]?'
        '\\d{4}\\b|\\bulr[-]?\\d{4}\\b|\\bumr[-]?\\d{4}\\b|\\bums[-]?\\d{4}\\b|\\bupr[-]?\\d{4}\\b'
    )
    assert affiliations_parsing._set_digits_keeping_prefix_regex() == expected_result


def test__search_dropping_digits_false():
    input_data = ["Humans first arrived in China during the Paleolithic.", "China", None]
    assert affiliations_parsing._search_dropping_digits(input_data) == [False]


def test__search_dropping_digits_true():
    input_data = ["fr-38 R22", "France", None]
    assert affiliations_parsing._search_dropping_digits(input_data, True) == [True]


def test__search_dropping_digits_keeping_prefix():
    input_data = ["fr-5678", "France", None]
    assert affiliations_parsing._search_dropping_digits(input_data, True) == [False]


def test__search_dropping_digits_dropping_zip_result():
    input_data = ["fr-67890", "France", None]
    assert affiliations_parsing._search_dropping_digits(input_data, True) == [True]


def test__search_dropping_suffix_true():
    assert affiliations_parsing._search_dropping_suffix(['campus'], True) == [True]


def test__search_dropping_suffix_false():
    assert affiliations_parsing._search_dropping_suffix(['aaa'], True) == [False]


def test__search_dropping_town_true():
    input_data = ["Universite Lyon 1", "France", {"France": ["universite lyon 1"]}]
    assert affiliations_parsing._search_dropping_town(input_data, True) == [True]


def test__search_dropping_town_false_case1():
    input_data = ["Christian Albrechts University", "Germany", {"France": ["universite lyon 1"]}]
    assert affiliations_parsing._search_dropping_town(input_data) == [False]


def test__search_dropping_town_false_case2():
    input_data = ["Universite Lyon 1", "Germany", {"France": ["bla", "blabla"]}]
    assert affiliations_parsing._search_dropping_town(input_data) == [False]


def test__search_dropping_words_false():
    input_data = ["L'Institut d'études avancées de l'université de Strasbourg", "France", None]
    assert affiliations_parsing._search_dropping_words(input_data) == [False]


def test__search_dropping_words_true_case_france():
    input_data = ["L'Institut d'études avancées de l'université de Strasbourg rue Blaise Pascal", "France", None]
    assert affiliations_parsing._search_dropping_words(input_data, True) == [True]


def test__search_dropping_words_true_case_other():
    input_data = ["Homi Bhabha National Institute street होमी भाभा नॅशनल इन्स्टिट्यूट", "India", None]
    assert affiliations_parsing._search_dropping_words(input_data, True) == [True]


def test__search_keeping_prefix_true():
    input_data = ["fr678", "France", None]
    assert affiliations_parsing._search_keeping_prefix(input_data, True) == [True]


def test__search_keeping_prefix_false_case1():
    input_data = ["fr678", "India", None]
    assert affiliations_parsing._search_keeping_prefix(input_data, True) == [False]


def test__search_keeping_prefix_false_case2():
    input_data = ["fr-678", "France", None]
    assert affiliations_parsing._search_keeping_prefix(input_data, True) == [False]


def test___search_keeping_words_all_false():
    input_data = ["bla bla bla"]
    assert affiliations_parsing._search_keeping_words(input_data) == [False, False, False]


def test___search_keeping_words_other_cases():
    input_data = ["CEA"]
    assert affiliations_parsing._search_keeping_words(input_data) == [False, False, True]

    input_data = ["CEA university"]
    assert affiliations_parsing._search_keeping_words(input_data) == [True, False, True]

    input_data = ["CEA university ea"]
    assert affiliations_parsing._search_keeping_words(input_data, True) == [True, True, True]


def test__search_items_1():
    found_item_tup = namedtuple('found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words'])

    assert affiliations_parsing._search_items(
        "L'Institut d'études avancées de l'université de Strasbourg",
        "France", {"France": ["universite lyon 1"]}
    ) == found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=False,
        gen_keeping_words=False, basic_keeping_words=False, user_keeping_words=False
    )


def test__search_items_2():
    found_item_tup = namedtuple('found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words'])

    assert affiliations_parsing._search_items(
        "CEA university ea",
        "France", {"France": ["universite lyon 1"]}
    ) == found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=False,
        gen_keeping_words=True, basic_keeping_words=True, user_keeping_words=True
    )


def test__check_dropping_digits_flag_case1():
    digits_drop_params = ["CEA university ea", [], "France", [], [], False]
    found_item_flags = affiliations_parsing._search_items(
        "CEA university ea", "France", {"France": ["universite lyon 1"]}
    )
    keeping_words_flags = [False]

    assert affiliations_parsing._check_dropping_digits_flag(
        digits_drop_params, found_item_flags, keeping_words_flags, True
    ) == ([], [('dropping_digits', [])], False, True)


def test__check_dropping_digits_flag_case2():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words']
    )

    digits_drop_params = ["CEA university ea", [], "France", [], [], False]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=True,
        gen_keeping_words=True, basic_keeping_words=False, user_keeping_words=False
    )
    keeping_words_flags = [False]

    assert affiliations_parsing._check_dropping_digits_flag(
        digits_drop_params, found_item_flags, keeping_words_flags, True
    ) == (['CEA university ea'], [], True, False)


def test__check_dropping_digits_flag_case3a():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words']
    )

    digits_drop_params = ["CEA university ea", [], "France", [], [], False]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=True,
        gen_keeping_words=False, basic_keeping_words=False, user_keeping_words=False
    )
    keeping_words_flags = [False]

    assert affiliations_parsing._check_dropping_digits_flag(
        digits_drop_params, found_item_flags, keeping_words_flags, True
    ) == (['CEA university ea'], [], True, False)


def test__check_dropping_digits_flag_case3b():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words']
    )

    digits_drop_params = ["CEA university ea", [], "France", [], [], True]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=True,
        gen_keeping_words=False, basic_keeping_words=False, user_keeping_words=False
    )
    keeping_words_flags = [False]

    assert affiliations_parsing._check_dropping_digits_flag(
        digits_drop_params, found_item_flags, keeping_words_flags, True
    ) == ([], [], True, False)


def test__check_dropping_digits_flag_case3c():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words']
    )

    digits_drop_params = ["CEA university ea", [], "France", [], [], True]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=True,
        gen_keeping_words=False, basic_keeping_words=True, user_keeping_words=True
    )
    keeping_words_flags = [False]

    assert affiliations_parsing._check_dropping_digits_flag(
        digits_drop_params, found_item_flags, keeping_words_flags, True
    ) == ([], [], True, False)


def test__check_dropping_digits_flag_case4():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words']
    )

    digits_drop_params = ["CEA university ea", [], "France", [], [], True]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=True,
        gen_keeping_words=False, basic_keeping_words=True, user_keeping_words=True
    )
    keeping_words_flags = [False]

    assert affiliations_parsing._check_dropping_digits_flag(
        digits_drop_params, found_item_flags, keeping_words_flags, True
    ) == ([], [], True, False)


def test__check_dropping_digits_flag_case5():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words']
    )

    digits_drop_params = ["CEA university ea", [], "Germany", [], [], True]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=True, keeping_prefix=False,
        gen_keeping_words=False, basic_keeping_words=True, user_keeping_words=False
    )
    keeping_words_flags = [False]

    assert affiliations_parsing._check_dropping_digits_flag(
        digits_drop_params, found_item_flags, keeping_words_flags, True
    ) == ([], [('dropping_digits', [])], True, True)


def test__check_dropping_digits_flag_case6():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words']
    )

    digits_drop_params = ["CEA university ea", [], "Germany", [], [], True]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=True, keeping_prefix=False,
        gen_keeping_words=False, basic_keeping_words=True, user_keeping_words=True
    )
    keeping_words_flags = [False]

    assert affiliations_parsing._check_dropping_digits_flag(
        digits_drop_params, found_item_flags, keeping_words_flags, True
    ) == ([], [('dropping_digits', [])], True, True)


def test__check_dropping_digits_flag_case7():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                                                   'dropping_town', 'dropping_words', 'keeping_prefix',
                                                   'gen_keeping_words', 'basic_keeping_words',
                                                   'user_keeping_words']
    )

    digits_drop_params = ["CEA university ea", [], "Germany", [], [], False]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=False,
        gen_keeping_words=False, basic_keeping_words=True, user_keeping_words=True
    )
    keeping_words_flags = [False]

    assert affiliations_parsing._check_dropping_digits_flag(
        digits_drop_params, found_item_flags, keeping_words_flags, True
    ) == (['CEA university ea'], [], True, False)


def test__check_dropping_words_flag_case1a():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                           'dropping_town', 'dropping_words', 'keeping_prefix',
                           'gen_keeping_words', 'basic_keeping_words',
                           'user_keeping_words']
    )

    words_drop_params = ["CEA university ea", [], [], [], True]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=False,
        gen_keeping_words=False, basic_keeping_words=False, user_keeping_words=False
    )

    assert affiliations_parsing._check_dropping_words_flag(
        words_drop_params, found_item_flags, [True], True
    ) == ([], [], True, False)


def test__check_dropping_words_flag_case1b():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                           'dropping_town', 'dropping_words', 'keeping_prefix',
                           'gen_keeping_words', 'basic_keeping_words',
                           'user_keeping_words']
    )

    words_drop_params = ["CEA university ea", [], [], [], False]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=False,
        gen_keeping_words=False, basic_keeping_words=False, user_keeping_words=False
    )

    assert affiliations_parsing._check_dropping_words_flag(
        words_drop_params, found_item_flags, [True], True
    ) == (['CEA university ea'], [], True, False)


def test__check_dropping_words_flag_case1c():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                           'dropping_town', 'dropping_words', 'keeping_prefix',
                           'gen_keeping_words', 'basic_keeping_words',
                           'user_keeping_words']
    )

    words_drop_params = ["CEA university ea", [], [], [], False]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=False,
        gen_keeping_words=True, basic_keeping_words=True, user_keeping_words=True
    )

    assert affiliations_parsing._check_dropping_words_flag(
        words_drop_params, found_item_flags, [True], True
    ) == (['CEA university ea'], [], True, False)


def test__check_dropping_words_flag_case2():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                           'dropping_town', 'dropping_words', 'keeping_prefix',
                           'gen_keeping_words', 'basic_keeping_words',
                           'user_keeping_words']
    )

    words_drop_params = ["CEA university ea", [], [], [], True]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=False,
        gen_keeping_words=False, basic_keeping_words=False, user_keeping_words=False
    )

    assert affiliations_parsing._check_dropping_words_flag(
        words_drop_params, found_item_flags, [False], True
    ) == ([], [('dropping_words', [])], False, True)


def test__check_dropping_suffix_flag_case1():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                           'dropping_town', 'dropping_words', 'keeping_prefix',
                           'gen_keeping_words', 'basic_keeping_words',
                           'user_keeping_words']
    )

    suffix_drop_params = ["CEA university ea", [], [], [], False]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=False,
        gen_keeping_words=True, basic_keeping_words=False, user_keeping_words=True
    )

    assert affiliations_parsing._check_dropping_suffix_flag(
        suffix_drop_params, found_item_flags, True
    ) == (['CEA university ea'], [], True, False)


def test__check_dropping_suffix_flag_case2():
    found_item_tup = namedtuple(
        'found_item_tup', ['dropping_bp', 'dropping_digits', 'dropping_suffix',
                           'dropping_town', 'dropping_words', 'keeping_prefix',
                           'gen_keeping_words', 'basic_keeping_words',
                           'user_keeping_words']
    )

    suffix_drop_params = ["CEA university ea", [], [], [], False]
    found_item_flags = found_item_tup(
        dropping_bp=False, dropping_digits=False, dropping_suffix=False,
        dropping_town=False, dropping_words=False, keeping_prefix=False,
        gen_keeping_words=False, basic_keeping_words=False, user_keeping_words=False
    )

    assert affiliations_parsing._check_dropping_suffix_flag(
        suffix_drop_params, found_item_flags, True
    ) == ([], [('dropping_suffix', [])], False, True)


def test__check_dropping_town_flag_case1():
    town_drop_params = ["CEA university ea", [], []]
    assert affiliations_parsing._check_dropping_town_flag(
        town_drop_params, True
    ) == ([('dropping_town', [])], True)


def test__check_dropping_town_flag_case2():
    town_drop_params = ["CEA university ea", [1, 2, 3], []]
    assert affiliations_parsing._check_dropping_town_flag(
        town_drop_params, True
    ) == ([('dropping_town', 'CEA university ea')], False)


def test__clean_affils_standard_case():
    affils_drop_params = ["CEA university ea", [], "France", [], []]
    towns_dict = {"France": ["CEA university ea"]}
    assert affiliations_parsing._clean_affils(
        affils_drop_params, towns_dict, True
    ) == (['CEA university ea'], [], False)


def test__clean_affils_empty_data_case():
    affils_drop_params = ["", [], "", [], []]
    towns_dict = {"": [""]}
    assert affiliations_parsing._clean_affils(
        affils_drop_params, towns_dict, True
    ) == ([], [('dropping_town', [])], True)


def test__clean_affils_dropping_bp_case():
    affils_drop_params = ["bp123", [], "", [], []]
    towns_dict = {"": [""]}
    assert affiliations_parsing._clean_affils(
        affils_drop_params, towns_dict, True
    ) == ([], [('dropping_bp', [])], True)


def test__clean_affils_dropping_digits_case():
    affils_drop_params = ["fr-38 R22", [], "France", [], []]
    towns_dict = {"": [""]}
    assert affiliations_parsing._clean_affils(
        affils_drop_params, towns_dict, True
    ) == ([], [('dropping_digits', [])], True)


def test__clean_affils_dropping_suffix_case():
    affils_drop_params = ["campus", [], "", [], []]
    towns_dict = {"": [""]}
    assert affiliations_parsing._clean_affils(
        affils_drop_params, towns_dict, True
    ) == ([], [('dropping_suffix', [])], True)


def test__clean_affils_dropping_words_case():
    affils_drop_params = [
        "L'Institut d'études avancées de l'université de Strasbourg rue Blaise Pascal", [], "France", [], []
    ]
    towns_dict = {"": [""]}
    assert affiliations_parsing._clean_affils(
        affils_drop_params, towns_dict, True
    ) == ([], [('dropping_words', [])], True)


def test__get_affils_list_vanilla():
    assert affiliations_parsing._get_affils_list(
        std_address="516 Jungong Rd, Yangpu District, China, 200093",
        towns_dict={"": [""]},
        drop_status=False,
        verbose=True
    ) == ('200093', ['516 Jungong Rd', 'Yangpu District', 'China'], [])


def test__get_affils_list_drop_status():
    assert affiliations_parsing._get_affils_list(
        std_address="516 Jungong Rd, Yangpu District, China, 200093",
        towns_dict={"": [""]},
        drop_status=True,
        verbose=True
    ) == ('200093', ['516 Jungong Rd'], [('dropping_words', [' Yangpu District', ' China'])])


def test__check_paris_univ():
    assert affiliations_parsing._check_paris_univ(
        address_norm_affiliations_set={"Univ Paris-Saclay", "Paris-Cité Univ"},
        verbose=True
    ) == {'Univ Paris-Saclay'}


def test__reorder_address_norm_affiliations():
    result = affiliations_parsing._reorder_address_norm_affiliations(
        {"Univ Paris-Saclay", "Paris-Cité"}, {'Paris-Saclay': '11', "Paris-Cité": "11"}, True
    )

    assert result == ['Univ Paris-Saclay', 'Paris-Cité'] or result == ['Paris-Cité', 'Univ Paris-Saclay']


def test__get_norm_affils_list():
    assert affiliations_parsing._get_norm_affils_list(
        "France",
        ['Univ Paris-Saclay', 'Paris-Cité'],
        {'France': {'Univ Paris-Saclay': '', 'Paris-Cité': ''}},
        {},
        True
    ) == ([], ['Univ Paris-Saclay', 'Paris-Cité'])


def test__build_addr_affils_lists():
    assert affiliations_parsing._build_addr_affils_lists(
        std_address="516 Jungong Rd, Yangpu District, China, 200093",
        affil_dicts={
            "affil_types_dict": {},
            "norm_raw_affils_dict": {"China": "", "200093": {}},
            "towns_dict": {"200093": ""}
        },
        drop_status=False,
        verbose=True
    ) == ('200093', [], ['516 Jungong Rd', 'Yangpu District', 'China'])


def test_build_addr_affils_tup_case1():
    affils_full_list_ntup = namedtuple(
        'affils_full_list_ntup',
        ['norm_affils_list', 'raw_affils_list']
    )

    assert affiliations_parsing.build_addr_affils_tup(
        full_address="516 Jungong Rd, Yangpu District, China",
        affil_params_dic={
            "affil_types_file_path": None,
            "country_affils_file_path": None,
            "country_towns_folder_path": None,
            "country_towns_file": None
        },
        drop_status=True
    ) == affils_full_list_ntup(
        norm_affils_list='empty', raw_affils_list='516 Jungong Rd'
    )


def test_build_addr_affils_tup_case2():
    affils_full_list_ntup = namedtuple(
        'affils_full_list_ntup',
        ['norm_affils_list', 'raw_affils_list']
    )

    assert affiliations_parsing.build_addr_affils_tup(
        full_address="Université Grenoble Alpes, 621 Av. Centrale, 38400 Saint-Martin-d'Hères, France",
        affil_params_dic={
            "affil_types_file_path": None,
            "country_affils_file_path": None,
            "country_towns_folder_path": None,
            "country_towns_file": None
        },
        drop_status=True
    ) == affils_full_list_ntup(norm_affils_list='UGA Univ', raw_affils_list='empty')


def test_build_norm_and_raw_affils():
    pass


def test_build_norm_affiliation_list():
    pass
