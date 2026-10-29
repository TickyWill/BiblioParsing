"""

"""
import pandas as pd

import bpfuncts.wos_references_parsing as wos_references_parsing

def test__clean_wos_ref():
    assert wos_references_parsing._clean_wos_ref("DOI [111], Ref1, Ref2") == 'DOI 111, Ref1, Ref2'


def test__find_wos_ref_doi():
    output =  wos_references_parsing._find_wos_ref_doi(
        ["DOI 10.0001/", "DOI 10.0002/", "DOI 10.0003/"]
    )

    assert isinstance(output, str)
    assert len(output.split(', ')) == 3
    assert 'DOI' not in output


def test__find_wos_ref_year():
    assert wos_references_parsing._find_wos_ref_year(["Reference", "2026"]) == '2026'


def test__set_wos_dotted_initials_case1a():
    assert wos_references_parsing._set_wos_dotted_initials('Jane D.') == ('Dotted', 'Jane D.')


def test__set_wos_dotted_initials_case1b():
    assert wos_references_parsing._set_wos_dotted_initials('von den bergen') == ('Dotted', 'von den b.')


def test__set_wos_dotted_initials_case2():
    assert wos_references_parsing._set_wos_dotted_initials('Jane D., John D.') == ('Dotted', 'Jane D., John.D.')


def test__find_wos_ref_authors_case1():
    assert wos_references_parsing._find_wos_ref_authors(["Anonymous"]) == ('Anonymous', 'Anonymous')


def test__find_wos_ref_authors_case2():
    assert wos_references_parsing._find_wos_ref_authors(["von den bergen"]) == ('Dotted', 'von den b.')


def test__search_journal_words_case1():
    assert wos_references_parsing._search_journal_words(
        'Journal 101', 'Title', 'Journal') == ('Title', 'Journal 101')


def test__search_journal_words_case2():
    assert wos_references_parsing._search_journal_words(
        '101', 'Title', 'Journal') == ('101', 'Journal')


def test__find_wos_ref_title_journal_case1():
    assert wos_references_parsing._find_wos_ref_title_journal(
        ['Jane D.'], '', '') == ('unknown', 'unknown')


def test__find_wos_ref_title_journal_case2():
    assert wos_references_parsing._find_wos_ref_title_journal(
        ['', 'Jane D.', 'John D.'], 'Anonymous', '2026') == ('John D.', 'unknown')


def test__find_wos_ref_title_journal_case3():
    assert wos_references_parsing._find_wos_ref_title_journal(
        ['', 'Jane D.', 'John D.'], 'Dotted', '2026') == ('unknown', 'John D.')


def test__find_wos_ref_title_journal_case4():
    assert wos_references_parsing._find_wos_ref_title_journal(
        ['', 'Jane D.', 'John D.'], '', '2026') == ('John D.', 'unknown')


def test__find_wos_ref_title_journal_case5():
    assert wos_references_parsing._find_wos_ref_title_journal(
        ['', 'Jane D.'], '', 'unknown') == ('Jane D.', 'unknown')


def test_build_wos_references():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/wos_corpus_df.csv')
    cols_tup = ({'articles_cols_list': ['Pub_id', 'Authors', 'Year', 'Journal', 'Volume', 'Page', 'DOI',
                                        'Document_type', 'Language', 'Title', 'ISSN'],
                 'address_cols_list': ['Pub_id', 'Idx_address', 'Address'],
                 'auth_cols_list': ['Pub_id', 'Idx_author', 'Co_author'],
                 'auth_affil_cols_list': ['Pub_id', 'Idx_author', 'Address', 'Country', 'Norm_institutions',
                                          'Raw_institutions', 'Secondary_institutions'],
                 'country_cols_list': ['Pub_id', 'Idx_address', 'Country'],
                 'affil_cols_list': ['Pub_id', 'Idx_address', 'Institution'], 'kw_cols_list': ['Pub_id', 'Keyword'],
                 'ref_cols_list': ['Pub_id', 'Authors', 'Year', 'Journal', 'DOI', 'Title', 'Full_reference'],
                 'tmp_cols_list': ['Title_LC', 'Dedup_Same_Journal', 'Title', 'title_tokens', 'kept_tokens',
                                   'doc_type_lc', 'doi_lc'], 'subject_cols_list': ['Pub_id', 'Subject'],
                 'sub_subject_cols_list': ['Pub_id', 'Sub_subject']},
                {'pub_id_col': 'Pub_id', 'subject_col': 'Subject', 'sub_subject_col': 'Sub_subject',
                 'affil_author_idx_col': 'Idx_author', 'norm_affils_col': 'Norm_institutions', 'address_col': 'Address',
                 'country_col': 'Country', 'affil_col': 'Institution', 'author_idx_col': 'Idx_author',
                 'co_authors_col': 'Co_author', 'keyword_col': 'Keyword', 'title_temp_col': 'Title',
                 'kept_tokens_col': 'kept_tokens', 'author_col': 'Authors', 'year_col': 'Year',
                 'doc_type_col': 'Document_type', 'title_col': 'Title', 'issn_col': 'ISSN',
                 'norm_journal_col': 'Norm_journal', 'wos_id_col': 'WoS_id'},
                {'wos_auth_col': 'AU', 'wos_title_kw_col': 'TI', 'wos_year_col': 'PY', 'wos_journal_col': 'SO',
                 'wos_volume_col': 'VL', 'wos_page_col': 'BP', 'wos_doi_col': 'DI', 'wos_aff_col': '',
                 'wos_auth_with_aff_col': 'C1', 'wos_auth_kw_col': 'DE', 'wos_idx_kw_col': 'ID', 'wos_ref_col': 'CR',
                 'wos_issn_col': 'SN', 'wos_language_col': 'LA', 'wos_doctype_col': 'DT', 'wos_fullnames_col': 'AF',
                 'wos_subjects_col': 'WC', 'wos_sub_subjects_col': 'SC', 'init_wos_id_col': 'UT'})
    output_df = wos_references_parsing.build_wos_references(input_df, cols_tup, True)

    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (1, 7)
