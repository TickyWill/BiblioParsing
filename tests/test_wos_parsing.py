"""

"""
import pandas as pd

import bpfuncts.wos_parsing as wos_parsing

def test__set_wos_parsing_cols():
    assert wos_parsing._set_wos_parsing_cols() == (
        {
            'articles_cols_list': [
                'Pub_id', 'Authors', 'Year', 'Journal', 'Volume', 'Page',
                'DOI', 'Document_type', 'Language', 'Title', 'ISSN'
            ],
            'address_cols_list': ['Pub_id', 'Idx_address', 'Address'],
            'auth_cols_list': ['Pub_id', 'Idx_author', 'Co_author'],
            'auth_affil_cols_list': [
                'Pub_id', 'Idx_author', 'Address', 'Country', 'Norm_institutions',
                'Raw_institutions', 'Secondary_institutions'
            ],
            'country_cols_list': ['Pub_id', 'Idx_address', 'Country'],
            'affil_cols_list': ['Pub_id', 'Idx_address', 'Institution'],
            'kw_cols_list': ['Pub_id', 'Keyword'],
            'ref_cols_list': [
                'Pub_id', 'Authors', 'Year', 'Journal', 'DOI', 'Title', 'Full_reference'
            ],
            'tmp_cols_list': [
                'Title_LC', 'Dedup_Same_Journal', 'Title', 'title_tokens', 'kept_tokens', 'doc_type_lc', 'doi_lc'
            ],
            'subject_cols_list': ['Pub_id', 'Subject'],
            'sub_subject_cols_list': ['Pub_id', 'Sub_subject']
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
            'norm_journal_col': 'Norm_journal',
            'wos_id_col': 'WoS_id'
        },
        {
            'wos_auth_col': 'AU',
            'wos_title_kw_col': 'TI',
            'wos_year_col': 'PY',
            'wos_journal_col': 'SO',
            'wos_volume_col': 'VL',
            'wos_page_col': 'BP',
            'wos_doi_col': 'DI',
            'wos_aff_col': '',
            'wos_auth_with_aff_col': 'C1',
            'wos_auth_kw_col': 'DE',
            'wos_idx_kw_col': 'ID',
            'wos_ref_col': 'CR',
            'wos_issn_col': 'SN',
            'wos_language_col': 'LA',
            'wos_doctype_col': 'DT',
            'wos_fullnames_col': 'AF',
            'wos_subjects_col': 'WC',
            'wos_sub_subjects_col': 'SC',
            'init_wos_id_col': 'UT'
        }
    )


def test__check_authors_list():
    output_tuple =  wos_parsing._check_authors_list(
        "Latargez, C; Durlin, Q; Vialle, C; Le Cocq, M; Schembri, A; Hartmann, JM; "
        "Crochemore, R; Lima, G; Grosse, P; André, L; Aliane, A",
        ""
    )

    assert isinstance(output_tuple, tuple)
    assert len(output_tuple) == 3


def test__set_upper_initials():
    assert wos_parsing._set_upper_initials("Jane Wheeler") == 'Jane WHEELER'


def test__build_wos_authors():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/wos_corpus_df.csv')
    fails_dic = {}
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
    output_df = wos_parsing._build_wos_authors(input_df, fails_dic, cols_tup)

    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (11, 3)

def test__build_wos_keywords():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/wos_corpus_df.csv')
    fails_dic = {}
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
    output = wos_parsing._build_wos_keywords(input_df, fails_dic, cols_tup)

    assert isinstance(output, tuple)
    assert len(output) == 3
    assert isinstance(output[0], pd.DataFrame)
    assert output[0].shape == (1, 2)
    assert isinstance(output[1], pd.DataFrame)
    assert output[1].shape == (1, 2)
    assert isinstance(output[2], pd.DataFrame)
    assert output[2].empty


def test__build_wos_addresses_countries_affiliations():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/wos_corpus_df.csv')
    fails_dic = {}
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
    output = wos_parsing._build_wos_addresses_countries_affiliations(input_df, fails_dic, cols_tup)

    assert isinstance(output, tuple)
    assert len(output) == 3
    assert isinstance(output[0], pd.DataFrame)
    assert output[0].shape == (1, 3)
    assert isinstance(output[1], pd.DataFrame)
    assert output[1].shape == (1, 3)
    assert isinstance(output[2], pd.DataFrame)
    assert output[2].shape == (1, 3)


def test__build_wos_authors_countries_affiliations():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/wos_corpus_df.csv')
    fails_dic = {}
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
    output_df = wos_parsing._build_wos_authors_countries_affiliations(input_df, fails_dic, cols_tup)

    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (11, 6)


def test__build_wos_articles():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/wos_corpus_df.csv')
    fails_dic = {}
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
    output_df = wos_parsing._build_wos_articles(input_df, fails_dic, cols_tup)

    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (1, 12)


def test_wos_parser():
    output = wos_parsing.wos_parser('./bpfuncts/RefFiles/wos')

    assert isinstance(output, tuple)
    assert len(output) == 3
    assert isinstance(output[0], dict)
    assert list(output[0].keys()) == [
        'articles', 'authors', 'addresses', 'countries', 'institutions', 'authors_institutions',
        'authors_keywords', 'indexed_keywords', 'title_keywords', 'subjects', 'sub_subjects', 'references'
    ]
    assert isinstance(output[1], dict)
    assert list(output[1].keys()) == [
        'number of article', 'Co_author', 'Address', 'Country', 'Institution',
        'Norm_institutions', 'Keyword', 'Subject', 'Sub_subject'
    ]
    assert isinstance(output[2], pd.DataFrame)
    assert output[2].shape == (1, 2)
