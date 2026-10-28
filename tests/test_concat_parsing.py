"""

"""
import pandas as pd

import bpfuncts.concat_parsing as concat_parsing

def test__set_dedup_cols():
    assert concat_parsing._set_dedup_cols() == {
        'pub_id_col': 'Pub_id',
        'authors_col': 'Authors',
        'page_col': 'Page',
        'doi_col': 'DOI',
        'doctype_col': 'Document_type',
        'title_col': 'Title',
        'issn_col': 'ISSN',
        'author_idx_col': 'Idx_author',
        'address_idx_col': 'Idx_address',
        'country_addr_idx_col': 'Idx_address',
        'inst_addr_idx_col': 'Idx_address',
        'auth_inst_auth_idx_col': 'Idx_author',
        'lc_title_col': 'Title_LC',
        'lc_doctype_col': 'doc_type_lc',
        'lc_doi_col': 'doi_lc',
        'same_journal_col': 'Dedup_Same_Journal',
        'norm_journal_col': 'Norm_journal'
    }


def test__concatenate_item_dfs():
    df1 = pd.DataFrame({"pub_id": [10, 44, 21]})
    df2 = pd.DataFrame({"pub_id": [30, 37]})
    pub_id_col = "pub_id"

    expected_result = pd.DataFrame({pub_id_col: [10, 21, 44, 75, 82]}, index=[0, 2, 1, 0, 1])
    assert concat_parsing._concatenate_item_dfs(df1, df2, pub_id_col).compare(expected_result).empty


def test_concatenate_parsing_case1():
    first_parsing_dict = {
        "authors_institutions": pd.DataFrame(
            {"Pub_id": [10, 44, 21],
             "Idx_author": [1, 2, 3],
             "Address": ["a1", "a2", "a3"],
             "Country": ["c1", "c2", "c3"],
             "Norm_institutions": ["ni1", "ni2", "ni3"]
             }
        )
    }

    second_parsing_dict = {
        "authors_institutions": pd.DataFrame(
            {"Pub_id": [30, 37],
            "Idx_author": [1, 2],
            "Address": ["a1", "a2"],
            "Country": ["c1", "c2"],
            "Norm_institutions": ["ni1", "ni2"]}
        )
    }

    result = concat_parsing.concatenate_parsing(
        first_parsing_dict,
        second_parsing_dict,
        [['a', 'b', 'c'], ['a', 'b', 'c']]
    )

    assert isinstance(result, dict)
    assert "authors_institutions" in result.keys()
    assert isinstance(result["authors_institutions"], pd.DataFrame)
    assert result["authors_institutions"].shape == (5, 7)


def test_concatenate_parsing_case2():
    first_parsing_dict = {'authors_institutions': pd.DataFrame()}

    second_parsing_dict = {
        "authors_institutions": pd.DataFrame(
            {"Pub_id": [30, 37],
            "Idx_author": [1, 2],
            "Address": ["a1", "a2"],
            "Country": ["c1", "c2"],
            "Norm_institutions": ["ni1", "ni2"]}
        )
    }

    result = concat_parsing.concatenate_parsing(
        first_parsing_dict,
        second_parsing_dict,
        [['a', 'b', 'c'], ['a', 'b', 'c']]
    )

    assert isinstance(result, dict)
    assert "authors_institutions" in result.keys()
    assert isinstance(result["authors_institutions"], pd.DataFrame)
    assert result["authors_institutions"].shape == (2, 7)


def test_concatenate_parsing_case3():
    first_parsing_dict = {
        "authors_institutions": pd.DataFrame(
            {"Pub_id": [10, 44, 21],
             "Idx_author": [1, 2, 3],
             "Address": ["a1", "a2", "a3"],
             "Country": ["c1", "c2", "c3"],
             "Norm_institutions": ["ni1", "ni2", "ni3"]
             }
        )
    }

    second_parsing_dict = {'authors_institutions': pd.DataFrame()}

    result = concat_parsing.concatenate_parsing(
        first_parsing_dict,
        second_parsing_dict,
        [['a', 'b', 'c'], ['a', 'b', 'c']]
    )

    assert isinstance(result, dict)
    assert "authors_institutions" in result.keys()
    assert isinstance(result["authors_institutions"], pd.DataFrame)
    assert result["authors_institutions"].shape == (3, 7)


def test_concatenate_parsing_case4():
    first_parsing_dict = {'authors_institutions': pd.DataFrame()}
    second_parsing_dict = {'authors_institutions': pd.DataFrame()}

    result = concat_parsing.concatenate_parsing(
        first_parsing_dict,
        second_parsing_dict,
        []
    )

    assert isinstance(result, dict)
    assert "authors_institutions" in result.keys()
    assert isinstance(result["authors_institutions"], pd.DataFrame)
    assert result["authors_institutions"].empty == True


def test__find_value_to_keep_case_other():
    assert concat_parsing._find_value_to_keep(pd.DataFrame({"values": ["a", "b", "c"]}), column_name="values") == 'a'


def test__find_value_to_keep_case_unknown():
    assert concat_parsing._find_value_to_keep(
        pd.DataFrame({"values": ["a", "b", "unknown"]}),
        column_name="values",
        length_max=True
    ) == 'a'


def test__norm_title():
    assert concat_parsing._norm_title(
        title="On Stability and Learning of Competitive Equilibrium "
              "in Generalized Fisher Market Models: A Variational Inequality Approach  "
    ) == ("On Stability and\xa0Learning of\xa0Competitive Equilibrium "
          "in\xa0Generalized Fisher Market Models: A Variational Inequality Approach")


def test__compute_similarity():
    assert concat_parsing._compute_similarity("Texte", "Text") == 89


def test__set_same_journal_name():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_result = concat_parsing._set_same_journal_name(input_df, "Title", "Same_title")

    assert isinstance(output_result, pd.DataFrame)
    assert output_result.shape == (180, 35)


def test__set_same_article_title():
    input_df = pd.read_csv('./bpfuncts/RefFiles/2026-06-16 scopus.csv')
    output_result = concat_parsing._set_same_article_title(
        input_df, "Source title", "Same_title")

    assert isinstance(output_result, pd.DataFrame)
    assert output_result.shape == (180, 35)


def test__set_issn():
    pass


def test__set_doi():
    pass


def test__set_doc_type():
    pass


def test__set_same_doi():
    pass


def test__set_same_first_author_name():
    pass


def test__drop_duplicate_article1():
    pass


def test__drop_duplicate_article2():
    pass


def test__norm_doctype():
    assert concat_parsing._norm_doctype('Article') == 'article'


def test__deduplicate_articles():
    pass


def test__deduplicate_item_df():
    pass


def test_deduplicate_parsing():
    pass
