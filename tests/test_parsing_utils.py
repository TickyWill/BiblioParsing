"""

"""

from bpfuncts.parsing_utils import *

def test_str_int_convertor_correct_string():
    result = str_int_convertor("123")
    assert result == 123


def test_str_int_convertor_incorrect_string():
    result = str_int_convertor("A")
    assert result == 0


def test_convert_issn():
    pass


def test_treat_doctype():
    pass


def test_treat_title():
    pass


def test_treat_author():
    pass


def test_set_unknown_address():
    pass


def test_check_and_get_rawdata_file_path():
    pass


def test_drop_rawdata():
    pass


def test_set_rawdata_error():
    pass


def test_build_item_df_from_tup():
    pass


def test_clean_authors_countries_affils():
    pass


def test__tokenizer():
    pass


def test_build_title_keywords():
    pass


def test_normalize_country():
    pass


def test_normalize_name():
    pass


def test_normalize_journal_names():
    pass


def test_build_pub_db_ids():
    pass


def test_check_and_drop_columns():
    pass


def test_upgrade_col_names():
    pass


def test_set_shared_parsing_cols():
    pass


def test_rationalize_town_names():
    pass


def test_standardize_str():
    pass


def test_set_address_uniform_words():
    pass


def test_standardize_address():
    pass
