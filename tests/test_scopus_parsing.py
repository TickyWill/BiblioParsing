"""

"""

import pandas as pd

import bpfuncts.scopus_parsing as scopus_parsing

def test__set_scopus_parsing_cols():
    assert scopus_parsing._set_scopus_parsing_cols() == (
        {
            'articles_cols_list': [
                'Pub_id',
                'Authors',
                'Year',
                'Journal',
                'Volume',
                'Page',
                'DOI',
                'Document_type',
                'Language',
                'Title',
                'ISSN'
            ],
            'address_cols_list': [
                'Pub_id',
                'Idx_address',
                'Address'
            ],
            'auth_cols_list': [
                'Pub_id',
                'Idx_author',
                'Co_author'
            ],
            'auth_affil_cols_list': [
                'Pub_id',
                'Idx_author',
                'Address',
                'Country',
                'Norm_institutions',
                'Raw_institutions',
                'Secondary_institutions'
            ],
            'country_cols_list': [
                'Pub_id',
                'Idx_address',
                'Country'
            ],
            'affil_cols_list': [
                'Pub_id',
                'Idx_address',
                'Institution'
            ],
            'kw_cols_list': [
                'Pub_id',
                'Keyword'
            ],
            'ref_cols_list': [
                'Pub_id',
                'Authors',
                'Year',
                'Journal',
                'DOI',
                'Title',
                'Full_reference'
            ],
            'tmp_cols_list': [
                'Title_LC',
                'Dedup_Same_Journal',
                'Title',
                'title_tokens',
                'kept_tokens',
                'doc_type_lc',
                'doi_lc']
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
            'scopus_id_col': 'Scopus_id'
        },
        {
            'scopus_auth_col': 'Authors',
            'scopus_title_kw_col': 'Title',
            'scopus_year_col': 'Year',
            'scopus_journal_col': 'Source title',
            'scopus_volume_col': 'Volume',
            'scopus_page_col': 'Page start',
            'scopus_doi_col': 'DOI',
            'scopus_aff_col': 'Affiliations',
            'scopus_auth_with_aff_col': 'Authors with affiliations',
            'scopus_auth_kw_col': 'Author Keywords',
            'scopus_idx_kw_col': 'Index Keywords',
            'scopus_ref_col': 'References',
            'scopus_issn_col': 'ISSN',
            'scopus_language_col': 'Language of Original Document',
            'scopus_doctype_col': 'Document Type',
            'scopus_fullnames_col': 'Author full names',
            'init_scopus_id_col': 'EID'
        }
    )


def test__set_author_idx():
    assert scopus_parsing._set_author_idx("Jane Doe", (1, "John Doe")) == (2, 'Jane Doe')


def test__get_author_affiliations_list():
    assert scopus_parsing._get_author_affiliations_list(
        "University Grenoble Alpes., CEA, Leti, DTIS, Grenoble, 38000, France; "
        "University Grenoble Alpes, CEA, Inserm, IRIG, UA13 BGE, Biomics, Grenoble, 38000, France; "
        "Cell Therapy Unit, AP-HP, Hopital Saint-Louis, Paris, F-75010, France; "
        "Research Unit of Strasbourg University Diabetes and Therapeutics , "
        "European Center for the Study of Diabetes (CeeD), UR7294, Strasbourg, 67200, France; "
        "ILONOV, Strasbourg, 67200, France",
        ["University Grenoble Alpes, CEA, Leti, DTIS, Grenoble, 38000, France",
         "University Grenoble Alpes, CEA, Inserm, IRIG, UA13 BGE, Biomics, Grenoble, 38000, France",
         "Cell Therapy Unit, AP-HP, Hopital Saint-Louis, Paris, F-75010, France",
         "Research Unit of Strasbourg University Diabetes and Therapeutics , "
         "European Center for the Study of Diabetes (CeeD), UR7294, Strasbourg, 67200, France",
         "ILONOV, Strasbourg, 67200, France"],
        (1, "Kozuch Sylvie")
    ) == (
        ['University Grenoble Alpes, CEA, Inserm, IRIG, UA13 BGE, Biomics, Grenoble, 38000, France',
         'Cell Therapy Unit, AP-HP, Hopital Saint-Louis, Paris, F-75010, France',
         'Research Unit of Strasbourg University Diabetes and Therapeutics, '
         'European Center for the Study of Diabetes (CeeD), UR7294, Strasbourg, 67200, France',
         'ILONOV, Strasbourg, 67200, France'],
        (2, 'University Grenoble Alpes.')
    )


def test__build_scopus_authors():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/corpus_df_parser.csv')
    fails_dic = {'number of article': 180}
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
                                   'doc_type_lc', 'doi_lc']},
                {'pub_id_col': 'Pub_id', 'subject_col': 'Subject', 'sub_subject_col': 'Sub_subject',
                 'affil_author_idx_col': 'Idx_author', 'norm_affils_col': 'Norm_institutions', 'address_col': 'Address',
                 'country_col': 'Country', 'affil_col': 'Institution', 'author_idx_col': 'Idx_author',
                 'co_authors_col': 'Co_author', 'keyword_col': 'Keyword', 'title_temp_col': 'Title',
                 'kept_tokens_col': 'kept_tokens', 'author_col': 'Authors', 'year_col': 'Year',
                 'doc_type_col': 'Document_type', 'title_col': 'Title', 'issn_col': 'ISSN',
                 'norm_journal_col': 'Norm_journal', 'scopus_id_col': 'Scopus_id'},
                {'scopus_auth_col': 'Authors', 'scopus_title_kw_col': 'Title', 'scopus_year_col': 'Year',
                 'scopus_journal_col': 'Source title', 'scopus_volume_col': 'Volume', 'scopus_page_col': 'Page start',
                 'scopus_doi_col': 'DOI', 'scopus_aff_col': 'Affiliations',
                 'scopus_auth_with_aff_col': 'Authors with affiliations', 'scopus_auth_kw_col': 'Author Keywords',
                 'scopus_idx_kw_col': 'Index Keywords', 'scopus_ref_col': 'References', 'scopus_issn_col': 'ISSN',
                 'scopus_language_col': 'Language of Original Document', 'scopus_doctype_col': 'Document Type',
                 'scopus_fullnames_col': 'Author full names', 'init_scopus_id_col': 'EID'})
    output_df = scopus_parsing._build_scopus_authors(input_df, fails_dic, cols_tup)

    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (1543, 3)


def test__build_scopus_keywords():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/corpus_df_parser.csv')
    fails_dic = {'number of article': 180}
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
                                   'doc_type_lc', 'doi_lc']},
                {'pub_id_col': 'Pub_id', 'subject_col': 'Subject', 'sub_subject_col': 'Sub_subject',
                 'affil_author_idx_col': 'Idx_author', 'norm_affils_col': 'Norm_institutions', 'address_col': 'Address',
                 'country_col': 'Country', 'affil_col': 'Institution', 'author_idx_col': 'Idx_author',
                 'co_authors_col': 'Co_author', 'keyword_col': 'Keyword', 'title_temp_col': 'Title',
                 'kept_tokens_col': 'kept_tokens', 'author_col': 'Authors', 'year_col': 'Year',
                 'doc_type_col': 'Document_type', 'title_col': 'Title', 'issn_col': 'ISSN',
                 'norm_journal_col': 'Norm_journal', 'scopus_id_col': 'Scopus_id'},
                {'scopus_auth_col': 'Authors', 'scopus_title_kw_col': 'Title', 'scopus_year_col': 'Year',
                 'scopus_journal_col': 'Source title', 'scopus_volume_col': 'Volume', 'scopus_page_col': 'Page start',
                 'scopus_doi_col': 'DOI', 'scopus_aff_col': 'Affiliations',
                 'scopus_auth_with_aff_col': 'Authors with affiliations', 'scopus_auth_kw_col': 'Author Keywords',
                 'scopus_idx_kw_col': 'Index Keywords', 'scopus_ref_col': 'References', 'scopus_issn_col': 'ISSN',
                 'scopus_language_col': 'Language of Original Document', 'scopus_doctype_col': 'Document Type',
                 'scopus_fullnames_col': 'Author full names', 'init_scopus_id_col': 'EID'})
    output = scopus_parsing._build_scopus_keywords(input_df, fails_dic, cols_tup)

    assert isinstance(output, tuple)
    assert len(output) == 3
    assert isinstance(output[0], pd.DataFrame)
    assert output[0].shape == (745, 2)
    assert isinstance(output[1], pd.DataFrame)
    assert output[1].shape == (3719, 2)
    assert isinstance(output[2], pd.DataFrame)
    assert output[2].shape == (523, 2)


def test__build_scopus_addresses_countries_affiliations():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/corpus_df_parser.csv')
    fails_dic = {'number of article': 180}
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
                                   'doc_type_lc', 'doi_lc']},
                {'pub_id_col': 'Pub_id', 'subject_col': 'Subject', 'sub_subject_col': 'Sub_subject',
                 'affil_author_idx_col': 'Idx_author', 'norm_affils_col': 'Norm_institutions', 'address_col': 'Address',
                 'country_col': 'Country', 'affil_col': 'Institution', 'author_idx_col': 'Idx_author',
                 'co_authors_col': 'Co_author', 'keyword_col': 'Keyword', 'title_temp_col': 'Title',
                 'kept_tokens_col': 'kept_tokens', 'author_col': 'Authors', 'year_col': 'Year',
                 'doc_type_col': 'Document_type', 'title_col': 'Title', 'issn_col': 'ISSN',
                 'norm_journal_col': 'Norm_journal', 'scopus_id_col': 'Scopus_id'},
                {'scopus_auth_col': 'Authors', 'scopus_title_kw_col': 'Title', 'scopus_year_col': 'Year',
                 'scopus_journal_col': 'Source title', 'scopus_volume_col': 'Volume', 'scopus_page_col': 'Page start',
                 'scopus_doi_col': 'DOI', 'scopus_aff_col': 'Affiliations',
                 'scopus_auth_with_aff_col': 'Authors with affiliations', 'scopus_auth_kw_col': 'Author Keywords',
                 'scopus_idx_kw_col': 'Index Keywords', 'scopus_ref_col': 'References', 'scopus_issn_col': 'ISSN',
                 'scopus_language_col': 'Language of Original Document', 'scopus_doctype_col': 'Document Type',
                 'scopus_fullnames_col': 'Author full names', 'init_scopus_id_col': 'EID'})
    output = scopus_parsing._build_scopus_addresses_countries_affiliations(input_df, fails_dic, cols_tup)

    assert isinstance(output, tuple)
    assert len(output) == 3
    assert isinstance(output[0], pd.DataFrame)
    assert output[0].shape == (583, 3)
    assert isinstance(output[1], pd.DataFrame)
    assert output[1].shape == (583, 3)
    assert isinstance(output[2], pd.DataFrame)
    assert output[2].shape == (583, 3)


def test__build_scopus_authors_countries_affiliations():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/corpus_df_parser_small.csv')
    fails_dic = {'number of article': 180}
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
                                   'doc_type_lc', 'doi_lc']},
                {'pub_id_col': 'Pub_id', 'subject_col': 'Subject', 'sub_subject_col': 'Sub_subject',
                 'affil_author_idx_col': 'Idx_author', 'norm_affils_col': 'Norm_institutions', 'address_col': 'Address',
                 'country_col': 'Country', 'affil_col': 'Institution', 'author_idx_col': 'Idx_author',
                 'co_authors_col': 'Co_author', 'keyword_col': 'Keyword', 'title_temp_col': 'Title',
                 'kept_tokens_col': 'kept_tokens', 'author_col': 'Authors', 'year_col': 'Year',
                 'doc_type_col': 'Document_type', 'title_col': 'Title', 'issn_col': 'ISSN',
                 'norm_journal_col': 'Norm_journal', 'scopus_id_col': 'Scopus_id'},
                {'scopus_auth_col': 'Authors', 'scopus_title_kw_col': 'Title', 'scopus_year_col': 'Year',
                 'scopus_journal_col': 'Source title', 'scopus_volume_col': 'Volume', 'scopus_page_col': 'Page start',
                 'scopus_doi_col': 'DOI', 'scopus_aff_col': 'Affiliations',
                 'scopus_auth_with_aff_col': 'Authors with affiliations', 'scopus_auth_kw_col': 'Author Keywords',
                 'scopus_idx_kw_col': 'Index Keywords', 'scopus_ref_col': 'References', 'scopus_issn_col': 'ISSN',
                 'scopus_language_col': 'Language of Original Document', 'scopus_doctype_col': 'Document Type',
                 'scopus_fullnames_col': 'Author full names', 'init_scopus_id_col': 'EID'})
    output_df = scopus_parsing._build_scopus_authors_countries_affiliations(input_df, fails_dic, cols_tup)
    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (21, 6)


def test__build_scopus_articles():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/corpus_df_parser.csv')
    fails_dic = {'number of article': 180}
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
                                   'doc_type_lc', 'doi_lc']},
                {'pub_id_col': 'Pub_id', 'subject_col': 'Subject', 'sub_subject_col': 'Sub_subject',
                 'affil_author_idx_col': 'Idx_author', 'norm_affils_col': 'Norm_institutions', 'address_col': 'Address',
                 'country_col': 'Country', 'affil_col': 'Institution', 'author_idx_col': 'Idx_author',
                 'co_authors_col': 'Co_author', 'keyword_col': 'Keyword', 'title_temp_col': 'Title',
                 'kept_tokens_col': 'kept_tokens', 'author_col': 'Authors', 'year_col': 'Year',
                 'doc_type_col': 'Document_type', 'title_col': 'Title', 'issn_col': 'ISSN',
                 'norm_journal_col': 'Norm_journal', 'scopus_id_col': 'Scopus_id'},
                {'scopus_auth_col': 'Authors', 'scopus_title_kw_col': 'Title', 'scopus_year_col': 'Year',
                 'scopus_journal_col': 'Source title', 'scopus_volume_col': 'Volume', 'scopus_page_col': 'Page start',
                 'scopus_doi_col': 'DOI', 'scopus_aff_col': 'Affiliations',
                 'scopus_auth_with_aff_col': 'Authors with affiliations', 'scopus_auth_kw_col': 'Author Keywords',
                 'scopus_idx_kw_col': 'Index Keywords', 'scopus_ref_col': 'References', 'scopus_issn_col': 'ISSN',
                 'scopus_language_col': 'Language of Original Document', 'scopus_doctype_col': 'Document Type',
                 'scopus_fullnames_col': 'Author full names', 'init_scopus_id_col': 'EID'})
    output_df = scopus_parsing._build_scopus_articles(input_df, fails_dic, cols_tup)

    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (180, 12)


def test_scopus_parser():
    output = scopus_parsing.scopus_parser('./bpfuncts/RefFiles/scopus_parser')

    assert isinstance(output, tuple)
    assert len(output) == 5
    assert isinstance(output[0], dict)
    assert list(output[0].keys()) == [
        'articles', 'authors', 'addresses', 'countries', 'institutions', 'authors_institutions',
        'authors_keywords', 'indexed_keywords', 'title_keywords', 'subjects', 'sub_subjects', 'references'
    ]
    assert isinstance(output[1], dict)
    assert output[1] == {
        'number of article': 1,
        'Co_author': {'success (%)': 100.0, 'Pub_id': []},
        'Address': {'success (%)': 100.0, 'Pub_id': []},
        'Country': {'success (%)': 100.0, 'Pub_id': []},
        'Institution': {'success (%)': 100.0, 'Pub_id': []},
        'Norm_institutions': {'success (%)': 100.0, 'Pub_id': []},
        'Keyword': {'success (%)': 100.0, 'Pub_id': []},
        'Subject': {'success (%)': 100.0, 'Pub_id': []},
        'Sub_subject': {'success (%)': 100.0, 'Pub_id': []}
    }
    assert isinstance(output[2], pd.DataFrame)
    assert output[2].shape == (1, 2)
    assert isinstance(output[3], pd.DataFrame)
    assert output[3].shape == (21, 3)
    assert isinstance(output[4], pd.DataFrame)
    assert output[4].empty
