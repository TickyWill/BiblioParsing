"""

"""
import pandas as pd

import bpfuncts.scopus_rawdata_utils as scopus_rawdata_utils

def test__set_scopus_rawdata_cols():
    assert scopus_rawdata_utils._set_scopus_rawdata_cols() == (
        {
            'scopus_id_col': 'Scopus_id',
            'pub_id_col': 'Pub_id'
        },
        {
            'scopus_auth_col': 'Authors',
            'scopus_aff_col': 'Affiliations',
            'scopus_auth_with_aff_col': 'Authors with affiliations',
            'scopus_fullnames_col': 'Author full names',
            'init_scopus_id_col': 'EID'
        }
    )


def test__check_authors_with_affiliations():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/corpus_df.csv')
    cols_list = ['Pub_id', 'Authors', 'Affiliations', 'Authors with affiliations']
    output = scopus_rawdata_utils._check_authors_with_affiliations(input_df, cols_list)

    assert isinstance(output, tuple)
    assert len(output) == 2
    assert isinstance(output[0], pd.DataFrame)
    assert output[0].shape == (180, 19)
    assert isinstance(output[1], pd.DataFrame)
    assert output[1].shape == (2, 10)


def test__correct_firstname_initials_case1():
    assert scopus_rawdata_utils._correct_firstname_initials("Doe, Jane (CEA)") == 'Doe J.'


def test__correct_firstname_initials_case2():
    assert scopus_rawdata_utils._correct_firstname_initials("Jane Doe") == 'Jane Doe U.F.N.'


def test__correct_auth_data():
    assert scopus_rawdata_utils._correct_auth_data(
        ("Doe, Jane (CEA)", "Affil1, Affil2, Affil3")
    ) == ('Doe J.', 'Doe J., Affil2, Affil3')


def test__check_authors():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/corpus_df.csv')
    check_cols = ['Pub_id', 'Authors', 'Author full names', 'Affiliations', 'Authors with affiliations']
    output = scopus_rawdata_utils._check_authors(input_df, check_cols)

    assert isinstance(output, tuple)
    assert len(output) == 2
    assert isinstance(output[0], pd.DataFrame)
    assert output[0].shape == (180, 19)
    assert isinstance(output[1], pd.DataFrame)
    assert output[1].shape == (1540, 3)


def test__correct_scopus_full_rawdata():
    input_df = pd.read_csv('./bpfuncts/RefFiles/corpus_data/corpus_df_copy.csv')
    cols_tup = ({'scopus_id_col': 'Scopus_id', 'pub_id_col': 'Pub_id'},
                {'scopus_auth_col': 'Authors', 'scopus_aff_col': 'Affiliations',
                 'scopus_auth_with_aff_col': 'Authors with affiliations', 'scopus_fullnames_col': 'Author full names',
                 'init_scopus_id_col': 'EID'})
    output = scopus_rawdata_utils._correct_scopus_full_rawdata(input_df, cols_tup)

    assert isinstance(output, tuple)
    assert len(output) == 3
    assert isinstance(output[0], pd.DataFrame)
    assert output[0].shape == (180, 19)
    assert isinstance(output[1], pd.DataFrame)
    assert output[1].shape == (1540, 3)
    assert isinstance(output[2], pd.DataFrame)
    assert output[2].shape == (2, 10)


def test_read_scopus_rawdata():
    output = scopus_rawdata_utils.read_scopus_rawdata(
        './bpfuncts/RefFiles/read_scopus_rawdata', True, False
    )

    assert isinstance(output, tuple)
    assert len(output) == 4
    assert isinstance(output[0], pd.DataFrame)
    assert output[0].shape == (180, 17)
    assert isinstance(output[1], pd.DataFrame)
    assert output[1].shape == (1540, 3)
    assert isinstance(output[2], pd.DataFrame)
    assert output[2].shape == (2, 10)
    assert isinstance(output[3], pd.DataFrame)
    assert output[3].empty
