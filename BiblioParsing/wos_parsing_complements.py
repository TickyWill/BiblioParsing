"""Module of functions for parsing of subjects and references fields 
of WoS rawdata.
"""

__all__ = ['build_wos_references',
           'build_wos_subjects_and_sub_subjects',
          ]


# Standard library imports
import re
from collections import namedtuple

# 3rd party library imports
import pandas as pd

# Local libray imports
import BiblioParsing.parsing_globals as bp_pg
import BiblioParsing.regex_globals as bp_rg

from BiblioParsing.parsing_utils import build_item_df_from_tup


def build_wos_subjects_and_sub_subjects(corpus_df, fails_dic, cols_tup):
    """Builds the data of subject per publication of the corpus 
    and updates the parsing success rate data.

    The structure of the built data is composed of 2 columns and one row 
    per publication and subject.
        Ex:
            Pub-index       Subject
               0       Neurosciences & Neurology
               1       Psychology
               1       Environmental Sciences & Ecology
               2       Engineering
               2       Physics
               3       Philosophy

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


def _clean_wos_ref(raw_ref):
    raw_ref_items_list = raw_ref.split(", ")
    ref_items_list = []
    for x in raw_ref_items_list:
        if "DOI [" in x:
            ref_items_list.append(x.replace(" [", " ").replace("]", "").replace("DOI DOI", "DOI "))
        else:
            ref_items_list.append(x.replace("[", "").replace("]", ""))
    ref = ", ".join(ref_items_list)
    return ref


def _try_list_idx(item_idx, value_idx, values_list):
    try:
        value_item_idx, value = item_idx, values_list[value_idx].strip()
    except IndexError:
        value_item_idx, value = 0, bp_pg.UNKNOWN
    return value_item_idx, value


def _find_wos_ref_doi(ref_items_list):
    dois_idx_list, init_dois_items_list = [], []
    for item_idx, ref_item in enumerate(ref_items_list):
        if re.findall(bp_rg.RE_WOS_REF_DOI, ref_item):
            dois_idx_list.append(item_idx)
            init_dois_items_list.append(ref_item)
    dois_list = list({x.replace("DOI","").strip().lower() for x in init_dois_items_list})
    dois_list_str = ", ".join(dois_list)
    return dois_list_str


def _find_wos_ref_year(ref_items_list):
    item_idx, years_list = 0, []
    for item_idx, ref_item in enumerate(ref_items_list):
        years_list = re.findall(bp_rg.RE_WOS_REF_YEAR, ref_item)
        if years_list:
            break
    _, year = _try_list_idx(item_idx, 0, years_list)
    return year


def _set_wos_dotted_initials(first_item):
    authors_case = "Undotted"
    mod_first_item = first_item
    initial_dot = '.'
    name_parts_list = first_item.split(" ")
    check_parts_list = list(name_parts_list)
    for part in re.findall(bp_rg.RE_AUTHORS_SMALL_WORDS, first_item):
        check_parts_list.remove(part.strip())
    name_check_nb = len(check_parts_list)
    if name_check_nb==2:
        lastname = ' '.join(name_parts_list[:-1])
        initials = name_parts_list[-1]
        if len(initials)<=4 and initials.isupper():
            dotted_initials = "".join([x + "." for x in initials.replace(".", "")])
        else:
            dotted_initials = initials[0] + initial_dot
        authors_case = "Dotted"
        mod_first_item = f'{lastname} {dotted_initials}'
    elif name_check_nb>2:
        lastname = ' '.join(name_parts_list[:-2])
        initials = name_parts_list[-2:]
        dotted_initials = "".join([x.replace(".", "") + initial_dot for x in initials])
        authors_case = "Dotted"
        mod_first_item = f'{lastname} {dotted_initials}'
    return authors_case, mod_first_item


def _find_wos_ref_authors(ref_items_list):
    first_item = ref_items_list[0]
    if "Anonymous" in first_item:
        authors = "Anonymous"
        authors_case = "Anonymous"
    else:
        authors_case, mod_first_item = _set_wos_dotted_initials(first_item)
        authors = mod_first_item
    return authors_case, authors


def _search_journal_words(item, title, journal):
    if re.findall(bp_rg.RE_WOS_REF_JOURNAL, item) or item.isupper():
        journal = item
    else:
        title = item
    return title, journal


def _find_wos_ref_title_journal(ref_items_list, authors_case, year):
    journal = bp_pg.UNKNOWN
    title = bp_pg.UNKNOWN
    ref_items_nb = len(ref_items_list)
    if ref_items_nb>2 and year!=bp_pg.UNKNOWN:
        second_item, third_item = ref_items_list[1], ref_items_list[2]
        if authors_case in ["Anonymous", "Undotted"]:
            title = third_item
        elif authors_case=="Dotted" and len(third_item)<=50:
            journal = third_item
        else:
            title, journal = _search_journal_words(third_item, title, journal)
    elif ref_items_nb==2 and year==bp_pg.UNKNOWN:
        second_item = ref_items_list[1]
        title, journal = _search_journal_words(second_item, title, journal)
    else:
        title = bp_pg.UNKNOWN
    return title, journal


def _build_wos_pub_refs_list(pub_id, ref_field, ref_cols_list, verbose):
    # Setting named tuple
    article_ref = namedtuple('article_ref', ref_cols_list)

    pub_refs_list =[]
    if isinstance(ref_field, str):
        # If the reference field is not empty and not an URL
        raw_refs_list = [x for x in ref_field.split("; ") if x]
        for raw_ref in raw_refs_list:
            if verbose:
                print("\n\nraw_ref       :", raw_ref)
            ref = _clean_wos_ref(raw_ref)
            ref_items_list = ref.split(", ")
            if verbose:
                print("ref           :", ref)
                print("ref_items_list:", ref_items_list)

            doi = _find_wos_ref_doi(ref_items_list)
            year = _find_wos_ref_year(ref_items_list)
            authors_case, authors = _find_wos_ref_authors(ref_items_list)
            title, journal = _find_wos_ref_title_journal(ref_items_list, authors_case, year)

            if verbose:
                print("    year          :", year)
                print("    authors       :", authors)
                print("    journal       :", journal)
                print("    doi           :", doi)
                print("    title         :", title)

            pub_refs_list.append(article_ref(pub_id, authors, year, journal, doi, title, raw_ref))
    return pub_refs_list


def build_wos_references(corpus_df, cols_tup, verbose=False):
    """Builds the data of cited references per publication of the corpus.

    The structure of the built data is composed of 6 columns and one row 
    per reference and per publication.
        Ex:

           Pub_id  Author     Year         Journal           Volume  Page
            0    Bellouard Q  2017   Int. J. Hydrog. Energy    42    13486
            0    Nishinaka H  2020   Energy Fuels              31    10933
            0    Bellouard Q  2018   Int. J. Hydrog. Energy    44    19193

    Args:
        corpus_df (dataframe): The selected rawdata of the corpus.
        cols_tup (tup): Columns information as built through \
        the `_set_wos_parsing_cols` internal function.
    Returns:
        (dataframe): The built data.
    """
    # Setting useful column names
    cols_lists_dic, cols_dic, wos_cols_dic = cols_tup
    ref_cols_list = cols_lists_dic['ref_cols_list']
    pub_id_col = cols_dic['pub_id_col']
    wos_ref_col = wos_cols_dic['wos_ref_col']

    refs_list =[]
    for pub_id, ref_field in zip(list(corpus_df[pub_id_col]), corpus_df[wos_ref_col]):
        if verbose:
            print("\n\npub_id:", pub_id)
        pub_refs_list = _build_wos_pub_refs_list(pub_id, ref_field, ref_cols_list, verbose)
        refs_list += pub_refs_list
    references_df = pd.DataFrame.from_dict({label:[s[idx] for s in refs_list]
                                            for idx, label in enumerate(ref_cols_list)})
    return references_df
