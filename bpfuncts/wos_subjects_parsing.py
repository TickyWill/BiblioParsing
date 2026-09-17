"""Module of functions for parsing of subjects fields of WoS rawdata.
"""

__all__ = ['build_wos_subjects_and_sub_subjects',
          ]


# Standard library imports
from collections import namedtuple

# Local libray imports
from bpfuncts.parsing_utils import build_item_df_from_tup


def build_wos_subjects_and_sub_subjects(corpus_df, fails_dic, cols_tup):
    """Builds the data of subject per publication of the corpus 
    and updates the parsing success rate data.

    The structure of the built data is composed of 2 columns and one row 
    per publication and subject.
    Ex:
    Pub-index       Subject.
    0           Neurosciences & Neurology.
    1           Psychology.
    1           Environmental Sciences & Ecology.
    2           Engineering.
    2           Physics.
    3           Philosophy.

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        fails_dic (dict): Parsing success rate data.
        cols_tup (tup): Columns information as built through \
        the `_set_wos_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    subject_cols_list = cols_lists_dic['subject_cols_list']
    sub_subject_cols_list = cols_lists_dic['sub_subject_cols_list']
    cols_keys = ['pub_id_col', 'subject_col', 'sub_subject_col']
    (pub_id_col, subject_col, sub_subject_col) = [cols_dic[key] for key in cols_keys]
    wos_subjects_col = wos_cols_dic['wos_subjects_col']
    wos_sub_subjects_col = wos_cols_dic['wos_sub_subjects_col']

    # Setting named tuples
    subject = namedtuple('subject', subject_cols_list)
    sub_subject = namedtuple('sub_subject', sub_subject_cols_list )

    corpus_series_zip = zip(corpus_df[pub_id_col], corpus_df[wos_subjects_col],
                            corpus_df[wos_sub_subjects_col])
    subjects_list, sub_subjects_list = [], []
    for pub_id, pub_subjects_str, pub_sub_subjects_str in corpus_series_zip:
        for pub_subject in pub_subjects_str.split(';'):
            subjects_list.append(subject(pub_id, pub_subject.strip()))
        if isinstance(pub_sub_subjects_str, str):
            for pub_sub_subject in pub_sub_subjects_str.split(';'):
                sub_subjects_list.append(sub_subject(pub_id, pub_sub_subject.strip()))

    # Building clean subjects and sub_subjects data and accordingly updating the parsing success rate dict
    subjects_df, fails_dic = build_item_df_from_tup(subjects_list, subject_cols_list,
                                                    subject_col, pub_id_col, fails_dic)
    sub_subjects_df, fails_dic = build_item_df_from_tup(sub_subjects_list, sub_subject_cols_list,
                                                        sub_subject_col, pub_id_col, fails_dic)
    return subjects_df, sub_subjects_df
