"""

"""

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
    pass


def test__get_author_affiliations_list():
    pass


def test__build_scopus_authors():
    pass


def test__build_scopus_keywords():
    pass


def test__build_scopus_addresses_countries_affiliations():
    pass


def test__build_scopus_authors_countries_affiliations():
    pass


def test__build_scopus_articles():
    pass


def test_scopus_parser():
    pass
