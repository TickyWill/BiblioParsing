"""

"""
from pathlib import WindowsPath

import pandas as pd

from bpfuncts.parsing_utils import *
from bpfuncts.wos_rawdata_utils import read_wos_rawdata
import bpfuncts.parsing_utils as parsing_utils

def test_try_list_idx_vanilla():
    assert try_list_idx(0, 0, ['value']) == (0, 'value')


def test_try_list_idx_exception():
    assert try_list_idx(0, 1, ['value']) == (0, 'unknown')


def test_str_int_convertor_correct_string():
    result = str_int_convertor("123")
    assert result == 123


def test_str_int_convertor_incorrect_string():
    result = str_int_convertor("A")
    assert result == 0


def test_convert_issn():
    assert convert_issn("11111111|1111|111") == '1111-11111111'


def test_treat_doctype():
    assert treat_doctype("Conference Paper") == "Conference paper"


def test_treat_title():
    assert treat_title(
        "On Stability and Learning of Competitive Equilibrium in Generalized Fisher Market Models: "
        "A Variational Inequality Approach"
    ) == ('On Stability and\xa0Learning of\xa0Competitive Equilibrium in\xa0'
          'Generalized Fisher Market Models: A Variational Inequality Approach')


def test_treat_author():
    assert treat_author(
        "Chergui B.; Aliane A.; Kaya H.; Porzier C.; Latargez C.; Licitra C.; Laulagnet F.; Kogelschatz M."
    ) == 'Chergui B'


def test_dict_print():
    assert dict_print({'key': 'value'}) is None


def test_set_unknown_address_auc_true():
    assert set_unknown_address(0, True) == '0_unknown, Unknown'


def test_set_unknown_address_auc_false():
    assert set_unknown_address(0, False) == '0_unknown'


def test_check_and_get_rawdata_file_path_vanilla():
    assert check_and_get_rawdata_file_path(
        './bpfuncts/RefFiles', 'xlsx'
    ) == WindowsPath('bpfuncts/RefFiles/scopus_parser/Scopus_IDs à supprimer.xlsx')


def test_check_and_get_rawdata_file_path_empty():
    assert check_and_get_rawdata_file_path('', 'xlsx') is None


def test_drop_rawdata():
    pass


def test_set_rawdata_error():
    pass


def test_build_item_df_from_tup():
    pass


def test_clean_authors_countries_affils():
    pass


def test__tokenizer():
    assert parsing_utils._tokenizer(
        'Thermal stability of Mg2Si0.55Sn0.45 for thermoelectric applications'
    ) == ['thermal', 'stability', 'mg2si0.55sn0.45', 'thermoelectric', 'application']


def test_build_title_keywords():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_results = build_title_keywords(input_df)

    assert isinstance(output_results, tuple)
    assert len(output_results) == 2
    assert isinstance(output_results[0], pd.DataFrame)
    assert output_results[0].shape == (180, 36)
    assert isinstance(output_results[1], list)
    assert len(output_results[1]) == 1024


def test_normalize_country_case1():
    assert normalize_country("Russia") == "Russian Federation"


def test_normalize_country_case2():
    assert normalize_country("china - China") == 'China'


def test_normalize_name_vanilla():
    assert normalize_name(
        "GrÔŁ-biçà-vèLU D'aillön, E-kj", firstname_only=True
    ) == "Grol-Bica-Velu D'Aillon E-KJ"


def test_normalize_name_case2():
    assert normalize_name(
        "McTavish Mason", firstname_only=True
    ) == 'McTavish Mason'


def test_normalize_journal_names_scopus():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_result = normalize_journal_names("scopus", input_df)

    assert isinstance(output_result, pd.DataFrame)
    assert output_result.shape == (180, 35)


def test_normalize_journal_names_wos():
    input_df = read_wos_rawdata('./bpfuncts/RefFiles/wos')[0]
    output_result = normalize_journal_names("wos", input_df)

    assert isinstance(output_result, pd.DataFrame)
    assert output_result.shape == (134, 19)


def test_normalize_journal_names_unrecognized_database():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_result = normalize_journal_names("", input_df)

    assert isinstance(output_result, pd.DataFrame)
    assert output_result.shape == (180, 34)


def test_build_pub_db_ids():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_df = build_pub_db_ids(input_df, 'Source', 'Source')

    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (180, 2)


def test_check_and_drop_columns_scopus():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_df = check_and_drop_columns("scopus", input_df)

    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (180, 16)


def test_check_and_drop_columns_unrecognized_database():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_df = check_and_drop_columns("", input_df)

    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (180, 34)


def test_upgrade_col_names():
    pass


def test_set_shared_parsing_cols():
    assert set_shared_parsing_cols() == (
        {
            'articles_cols_list': [
                'Pub_id', 'Authors', 'Year', 'Journal',
                'Volume', 'Page', 'DOI', 'Document_type',
                'Language', 'Title', 'ISSN'
            ],
            'address_cols_list': [
                'Pub_id', 'Idx_address', 'Address'
            ],
            'auth_cols_list': [
                'Pub_id', 'Idx_author', 'Co_author'
            ],
            'auth_affil_cols_list': [
                'Pub_id', 'Idx_author', 'Address', 'Country',
                'Norm_institutions', 'Raw_institutions', 'Secondary_institutions'
            ],
            'country_cols_list': [
                'Pub_id', 'Idx_address', 'Country'
            ],
            'affil_cols_list': [
                'Pub_id', 'Idx_address', 'Institution'
            ],
            'kw_cols_list': [
                'Pub_id', 'Keyword'
            ],
            'ref_cols_list': [
                'Pub_id', 'Authors', 'Year', 'Journal', 'DOI', 'Title', 'Full_reference'
            ],
            'tmp_cols_list': [
                'Title_LC', 'Dedup_Same_Journal', 'Title', 'title_tokens',
                'kept_tokens', 'doc_type_lc', 'doi_lc'
            ]
        },
        {
            'pub_id_col': 'Pub_id',
            'subject_col': 'Subject',
            'sub_subject_col': 'Sub_subject',
            'affil_author_idx_col': 'Idx_author',
            'norm_affils_col': 'Norm_institutions',
            'address_col': 'Address',
            'country_col': 'Country',
            'affil_col': 'Institution',
            'author_idx_col': 'Idx_author',
            'co_authors_col': 'Co_author',
            'keyword_col': 'Keyword',
            'title_temp_col': 'Title',
            'kept_tokens_col': 'kept_tokens',
            'author_col': 'Authors',
            'year_col': 'Year',
            'doc_type_col': 'Document_type',
            'title_col': 'Title',
            'issn_col': 'ISSN',
            'norm_journal_col': 'Norm_journal'
        }
    )


def test_rationalize_town_names():
    assert rationalize_town_names("Grenoble") == "Grenoble"


def test_standardize_str():
    assert standardize_str(
        "High-speed non-volatile barium titanate field-programmable photonic gate array"
    ) == 'High-speed non-volatile barium titanate field-programmable photonic gate array'


def test_set_address_uniform_words():
    assert set_address_uniform_words(
        "17 Av. des Martyrs, 38000 Grenoble"
    ) == '17 Av. des Martyrs, 38000 Grenoble'


def test_standardize_address_vanilla():
    assert standardize_address('17 Av. des Martyrs,38000 Grenoble,Fr') == '17 Av. Martyrs,38000 Grenoble, France'


def test_standardize_address_unknown_country_added():
    assert standardize_address(
        '"17 Av. des Martyrs,38000 Grenoble,Aa', True
    ) == '17 Av. Martyrs,38000 Grenoble,Aa, Unknown'


def test_standardize_address_unknown_country_not_added():
    assert standardize_address(
        '"17 Av. des Martyrs,38000 Grenoble,Aa', False
    ) == '17 Av. Martyrs,38000 Grenoble,Aa'
