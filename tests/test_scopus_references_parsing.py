"""

"""

import pandas as pd

import bpfuncts.scopus_references_parsing as scopus_references_parsing

def test__find_scopus_ref_doi():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    input_list = list(input_df['DOI'])
    output_result =  scopus_references_parsing._find_scopus_ref_doi(input_list, True)

    assert isinstance(output_result, tuple)
    assert output_result == (0, '10.1116/6.0005192')


def test__find_scopus_ref_year_case1():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    input_df['ref_items'] = input_df['DOI'] + ' - .2038.'
    output_result = scopus_references_parsing._find_scopus_ref_year(
        list(input_df['ref_items']), 0, '10.1116/6.0005192', '', True
    )

    assert isinstance(output_result, tuple)
    assert output_result == (0, '2038')


def test__find_scopus_ref_year_case2():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    input_df['ref_items'] = input_df['DOI'] + ' - .2038.'
    output_result = scopus_references_parsing._find_scopus_ref_year(
        list(input_df['ref_items']), 0, 'unknown', '', True
    )

    assert isinstance(output_result, tuple)
    assert output_result == (0, '1116')


def test__find_scopus_ref_year_case3():
    input_list = ["a Conference a"] * 3
    output_result = scopus_references_parsing._find_scopus_ref_year(
        list(input_list), 0, '10.1116/6.0005192', '', True
    )

    assert isinstance(output_result, tuple)
    assert output_result == (0, 'unknown')


def test__find_scopus_ref_year_case4():
    input_list = ["Conference (2018)"]
    output_result = scopus_references_parsing._find_scopus_ref_year(
        list(input_list), 0, '10.1116/6.0005192', '', True
    )

    assert isinstance(output_result, tuple)
    assert output_result == (0, '2018')


def test__check_author_in_next_items():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    assert scopus_references_parsing._check_author_in_next_items(
        0, 1, 10, list(input_df["Authors"]), True
    ) == 10


def test__check_authors_case_w_dot():
    assert scopus_references_parsing._check_authors_case_w_dot(
        "author", "item", True
    ) == 'author_wo_dot'


def test__set_one_item_authors():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_result =  scopus_references_parsing._set_one_item_authors(
        'item1', 'item A', 3, list(input_df["Authors"]), 'Ph.D', True
    )

    assert isinstance(output_result, tuple)
    assert output_result == ('item1', 'et al.', 'multiple_one_item_wo_dot', 3)


def test__set_two_item_authors():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_result = scopus_references_parsing._set_two_item_authors(
        'item1', 'item A', 1e6, list(input_df["Authors"]), 'Ph.D', True
    )

    assert isinstance(output_result, tuple)
    assert output_result == ('item1 item A', 'et al.', 'multiple_two_items_w_dot', 151)


def test__search_authors_in_items_case1():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_result = scopus_references_parsing._search_authors_in_items(input_df["Authors"], True)

    assert isinstance(output_result, tuple)
    assert output_result == ('Kozuch S.; Ruel S.; Vaufrey D.; Renault O.', 'et al.', 'multiple_one_item_w_dot', 152)


def test__search_authors_in_items_case2():
    input_list = ['', 'Kozuch S.; Ruel S.; Vaufrey D.; Renault O.']
    output_result = scopus_references_parsing._search_authors_in_items(input_list, True)

    assert isinstance(output_result, tuple)
    assert output_result == (' Kozuch S.; Ruel S.; Vaufrey D.; Renault O.', '', 'single_two_items', 1)


def test__build_authors_attr_case1():
    input_list = ['Et al.']
    assert scopus_references_parsing._build_authors_attr(input_list, True) == ('Et al.', '', 'with_et_al', 0)


def test__build_authors_attr_case2():
    input_list = ['Hut M.; Clément F.; Tubbs E.; Caullet P.; Armanet M.; Domet T.; '
                  'Bouzakri K.; Bietiger W.; Navarro F.P.; Fouillet Y.; Agache V.']
    assert scopus_references_parsing._build_authors_attr(
        input_list, True
    ) == ('unknown', '', 'first_item_too_long', 0)


def test__build_authors_attr_case3():
    input_list = ['Hut M.; Clément F.; Tubbs E.; Caullet P.; Armanet M.; Domet T.; '
                  'Bouzakri K.; Bietiger W.; Navarro F.P.; Fouillet Y.; Agache V.', 'Hut M.; Clément F.; et al.']
    assert scopus_references_parsing._build_authors_attr(
        input_list, True
    ) == ('Hut M.; Clément F.; et al.', 'etc.', 'partial_one_item', 0)


def test__build_authors_attr_case4():
    input_list = ['', 'Kozuch S.; Ruel S.; Vaufrey D.; Renault O.']
    assert scopus_references_parsing._build_authors_attr(
        input_list, True
    ) == (' Kozuch S.; Ruel S.; Vaufrey D.; Renault O.', '', 'single_two_items', 1)


def test__set_scopus_dotted_initials():
    assert scopus_references_parsing._set_scopus_dotted_initials(
        'Vaufrey D', True
    ) == 'Vaufrey D.'


def test__find_scopus_ref_authors():
    input_list = ['Et al.']
    assert scopus_references_parsing._find_scopus_ref_authors(
        input_list, True
    ) == ('Et al. ', 'with_et_al', 0)


def test__find_scopus_ref_title_case1():
    input_list = ['Hut M.; Clément F.;', 'Tubbs E.; Caullet P.; Armanet M.; Domet T.;',
                  'Bouzakri K.; Bietiger W.; Navarro F.P.; Fouillet Y.; Agache V.', 'A. -a. ']
    search_title_params = ['multiple_two_items_w_dot', 2018, 0, '', 0, 2]
    assert scopus_references_parsing._find_scopus_ref_title(
        input_list, search_title_params, True
    ) == (0, 'unknown')


def test__find_scopus_ref_title_case2():
    input_list = ['Hut M.; Clément F.;', 'Tubbs E.; Caullet P.; Armanet M.; Domet T.;',
                  'Bouzakri K.; Bietiger W.; Navarro F.P.; Fouillet Y.; Agache V.', 'A. -a. ']
    search_title_params = ['no_authors', 2018, 0, '', 0, 2]
    assert scopus_references_parsing._find_scopus_ref_title(
        input_list, search_title_params, True
    ) == (0, 'unknown')


def test__find_scopus_ref_title_case3():
    input_list = ['(2018)']
    search_title_params = ['no_authors', 2018, 0, '', 0, 2]
    assert scopus_references_parsing._find_scopus_ref_title(
        input_list, search_title_params, True
    ) == (0, 'unknown')


def test__split_long_item_by_dot():
    assert scopus_references_parsing._split_long_item_by_dot("Item. to split") == ('Item', 'to split')


def test__select_journal_part_case1():
    assert scopus_references_parsing._select_journal_part(
        "Journal - CH4/H2/Ar etching study: N-polar GaN for micro-LED applications",
        True, "", True
    ) == ('Journal - CH4/H2/Ar etching study', 'Journal - CH4/H2/Ar etching study')


def test__select_journal_part_case2():
    assert scopus_references_parsing._select_journal_part(
        "Journal - CH4/H2/Ar etching study: N-polar GaN in micro-LED applications",
        False, "", True
    ) == ('Journal - CH4/H2/Ar etching study: N-polar GaN in micro-LED applications',
          'Journal - CH4/H2/Ar etching study: N-polar GaN in micro-LED applications')


def test__select_journal_part_case3():
    assert scopus_references_parsing._select_journal_part(
        "CH4/H2/Ar etching study: N-polar GaN in micro-LED applications Journal",
        False, "", True
    ) == ('in micro-LED applications Journal', 'micro-LED applications Journal')


def test__select_journal_part_case4():
    assert scopus_references_parsing._select_journal_part(
        "CH4/H2/Ar etching study: N-polar GaN in micro-LED applications, "
        "Modelling of magnetic vortex in Letters ",
        False, "", True
    ) == ('in Letters ', 'Letters ')


def test__select_journal_part_case5():
    assert scopus_references_parsing._select_journal_part(
        "Modelling of magnetic vortex in theory (2018)", False, "", True
    ) == ('2018)', ')')


def test__check_item_date():
    assert scopus_references_parsing._check_item_date("(2026)")


def test__try_next_items_vanilla():
    assert scopus_references_parsing._try_next_items(
        ["Ref1", "Ref2", "Ref3"], 1, "Ref2"
    ) == (2, 'Ref3')


def test__try_next_items_case_unknown():
    assert scopus_references_parsing._try_next_items(
        ["Ref1", "Ref2", "Ref3"], 2, "Ref3"
    ) == (2, 'unknown')


def test__clean_journal_and_title_case1():
    assert scopus_references_parsing._clean_journal_and_title(
        ["Entrée de journal", 0, "Entrée de journal", "DOI", ['Part1', 'Part2'], ['Item1', 'Item2']]
    ) == ('Part1 - Part2', 'Item2')


def test__clean_journal_and_title_case2():
    assert scopus_references_parsing._clean_journal_and_title(
        ["Entrée de journal", 0, "Awsome Title", "Entrée de journal", ['Part1', 'Part2'], ['Item1', 'Item2']]
    ) == ('unknown', 'Awsome Title')


def test__clean_journal_and_title_case3():
    assert scopus_references_parsing._clean_journal_and_title(
        ["Title", 0, "TitleTitle", "Entrée de journal", ['Part1', 'Part2'], ['Item1', 'Item2']]
    ) == ('Title', 'unknown')


def test__clean_journal_and_title_case4():
    assert scopus_references_parsing._clean_journal_and_title(
        ["unknown", 0, "Titre.Oeuvre", "Entrée de journal", ['Part1', 'Part2'], ['Item1', 'Item2']]
    ) == ('Titre.Oeuvre', 'unknown')


def test__find_scopus_ref_journal_case1a():
    input_list = ['Entry1', 'Entry2', 'Entry3']
    search_journal_params = ['first_item_too_long', 0, 'Main. Title: Journal', 'DOI', 10]
    assert scopus_references_parsing._find_scopus_ref_journal(
        input_list, search_journal_params, True
    ) == (0, ' Journal', 'Main')


def test__find_scopus_ref_journal_case1b():
    input_list = ['Entry1', 'Journal 1', 'Entry3']
    search_journal_params = ['first_item_too_long', 0, 'Main. Title', 'DOI', 10]
    assert scopus_references_parsing._find_scopus_ref_journal(
        input_list, search_journal_params, True
    ) == (1, 'Journal 1', 'Main')


def test__find_scopus_ref_journal_case2():
    input_list = ['Journal. 1a.', 'Journal. 2a.', 'Journal 1']
    search_journal_params = ['', 0, 'Main. Title', 'DOI', 0]
    assert scopus_references_parsing._find_scopus_ref_journal(
        input_list, search_journal_params, True
    ) == (1, 'Journal. 2a.', 'Main. Title')


def test__find_scopus_ref_journal_case3():
    input_list = ['Journal. 1', 'Journal. 2', 'Journal. 3']
    search_journal_params = ['', 0, 'Main. Title', 'DOI', 0]
    assert scopus_references_parsing._find_scopus_ref_journal(
        input_list, search_journal_params, True
    ) == (1, 'Journal. 2', 'Main. Title')


def test__merge_ref_item():
    assert scopus_references_parsing._merge_ref_item(
        'item: value', ['Ref1', 'Ref2 and Ref 2a', 'Ref3'], 0, 'new'
    ) == (1, 'new Ref2 and Ref 2a')


def test__check_merge_ref_items_colon():
    assert scopus_references_parsing._check_merge_ref_items_colon(
        0, 'Reference: Item',
        ['Item1', 'Item2 and Item2a', 'Item3'], 'Journal, and publication'
    ) == (1, 'Reference: Item Item2 and Item2a')


def test__count_upper_len_():
    assert scopus_references_parsing._count_upper_len("Texte de référence") == 1


def test__cases_check():
    assert scopus_references_parsing._cases_check("Texte de référence") == (False, False)


def test__check_merge_ref_items_lowercase():
    assert scopus_references_parsing._check_merge_ref_items_lowercase(
        0, 'reference item', ['item1', 'item2', 'Item3']
    ) == (1, 'reference item item2')


def test__drop_ref_items():
    assert scopus_references_parsing._drop_ref_items(
        r'\sand\s', ['and', ' and ', ' and']
    ) == ['and', ' and']


def test__check_move_first_item():
    assert scopus_references_parsing._check_move_first_item(
        r'\sand\s', [' and ', 'and ']
    ) == ['and ', ' and ']


def test__drop_all_items_after():
    assert scopus_references_parsing._drop_all_items_after(
        r'\sand\s', [' and ', 'and ']
    ) == [' and ']


def test__clean_ref():
    assert scopus_references_parsing._clean_ref(
        "Lee V.W.; Twu N.; Kymissis I., Inf. Disp., 32, (2016); Lee T.-Y., ACS Photonics, 9, (2022)"
    ) == 'Lee V.W.; Twu N.; Kymissis I., Inf. Disp., 32, (2016); Lee T.-Y., ACS Photonics, 9, (2022)'


def test_build_scopus_references():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/corpus_df.csv')
    cols_tup = (
        {
            'articles_cols_list': [
                'Pub_id', 'Authors', 'Year', 'Journal', 'Volume', 'Page',
                'DOI', 'Document_type', 'Language', 'Title', 'ISSN'
            ],
            'address_cols_list': [
                'Pub_id', 'Idx_address', 'Address'
            ],
            'auth_cols_list': [
                'Pub_id', 'Idx_author', 'Co_author'
            ],
            'auth_affil_cols_list': [
                'Pub_id', 'Idx_author', 'Address', 'Country', 'Norm_institutions',
                'Raw_institutions', 'Secondary_institutions'
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
                'Title_LC', 'Dedup_Same_Journal', 'Title', 'title_tokens', 'kept_tokens', 'doc_type_lc', 'doi_lc'
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
            'norm_journal_col':  'Norm_journal',
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

    output_df = scopus_references_parsing.build_scopus_references(input_df, cols_tup, 0, 0)
    assert isinstance(output_df, pd.DataFrame)
    assert output_df.shape == (28144, 7)
