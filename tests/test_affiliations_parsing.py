"""

"""
import re
from collections import namedtuple

import bpfuncts.affiliations_parsing as affiliations_parsing

def test__set_norm_affils_cols():
    expected_result = (
        {
            'country_cols_list': ['Pub_id', 'Idx_address', 'Country'],
            'affil_cols_list': ['Pub_id', 'Idx_address', 'Institution'],
            'auth_affil_cols_list': [
                'Pub_id', 'Idx_author', 'Address', 'Country',
                'Norm_institutions', 'Raw_institutions', 'Secondary_institutions']
        },
        {
            'pub_id_col': 'Pub_id',
            'address_id_col': 'Idx_address',
            'address_col': 'Address',
            'country_col': 'Country',
            'affil_col': 'Institution',
            'norm_affils_col': 'Norm_institutions'
        }
    )
    assert affiliations_parsing._set_norm_affils_cols() == expected_result


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


def test__check_dropping_digits_flag():
    pass


def test__check_dropping_words_flag():
    pass


def test__check_dropping_suffix_flag():
    pass


def test__check_dropping_town_flag():
    pass


def test__clean_affils():
    pass


def test__get_affils_list():
    pass


def test__build_norm_affiliation_list():
    pass


def test__check_paris_univ():
    pass


def test__reorder_address_norm_affiliations():
    pass


def test__get_norm_affils_list():
    pass


def test__build_addr_affils_lists():
    pass


def test_build_addr_affils_tup():
    pass


def test__build_complements_list():
    pass


def test_extend_author_affils():
    pass


def test_build_norm_and_raw_affils():
    pass
