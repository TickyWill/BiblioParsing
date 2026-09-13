"""Module of functions for parsing of subjects and references fields 
of WoS rawdata.
"""

__all__ = ['build_wos_references',
          ]


# Standard library imports
import re
from collections import namedtuple

# 3rd party library imports
import pandas as pd

# Local libray imports
import bpfuncts.parsing_globals as bp_pg
import bpfuncts.regex_globals as bp_rg
from bpfuncts.parsing_utils import try_list_idx


def _clean_wos_ref(raw_ref):
    """Cleans the reference when it contains '[' or ']'.

    Args:
        raw_ref (str): The reference to clean.
    Returns:
        (str): The cleaned reference.
    """
    raw_ref_items_list = raw_ref.split(", ")
    ref_items_list = []
    for x in raw_ref_items_list:
        if "DOI [" in x:
            ref_items_list.append(x.replace(" [", " ").replace("]", "").replace("DOI DOI", "DOI "))
        else:
            ref_items_list.append(x.replace("[", "").replace("]", ""))
    ref = ", ".join(ref_items_list)
    return ref


def _find_wos_ref_doi(ref_items_list):
    """Searches for the DOI in the items resulting from the split by ', ' of a reference 
    using the regex 'RE_WOS_REF_DOI', a global imported from the `bpfuncts.regex_globals` module.

    Args:
        ref_items_list (list): The items (str) of the reference.
    Returns:
        (str): A txt gathering items with DOI info.
    """
    init_dois_items_list = []
    for ref_item in ref_items_list:
        if re.findall(bp_rg.RE_WOS_REF_DOI, ref_item):
            init_dois_items_list.append(ref_item)
    dois_list = list({x.replace("DOI","").strip().lower() for x in init_dois_items_list})
    dois_list_str = ", ".join(dois_list)
    return dois_list_str


def _find_wos_ref_year(ref_items_list):
    """Searches for the year in the items resulting from the split 
    by ', ' of a reference using the regex 'RE_WOS_REF_YEAR', 
    a global imported from the `bpfuncts.regex_globals` module.

    Args:
        ref_items_list (list): The items (str) of the reference.
    Returns:
        (str): The first occurrence of found years.
    """
    item_idx, years_list = 0, []
    for item_idx, ref_item in enumerate(ref_items_list):
        years_list = re.findall(bp_rg.RE_WOS_REF_YEAR, ref_item)
        if years_list:
            break
    _, year = try_list_idx(item_idx, 0, years_list)
    return year


def _set_wos_dotted_initials(first_item):
    """Modifies the first item of the items resulting from the split 
    by ', ' of a reference in order to set dotted initials for the authors.

    Args:
        first_item (str): The first item of the reference.
    Returns:
        (tup): Composed of the case of the authors initials (str) \
        and of the modified item (str).
    """
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
    """Sets the authors from the first item of the items resulting 
    from the split by ', ' of a reference.

    Args:
        ref_items_list (list): The items (str) of the reference.
    Returns:
        (tup): Composed of the case of the authors initials (str) \
        and of the authors (str).
    """
    first_item = ref_items_list[0]
    if "Anonymous" in first_item:
        authors = "Anonymous"
        authors_case = "Anonymous"
    else:
        authors_case, mod_first_item = _set_wos_dotted_initials(first_item)
        authors = mod_first_item
    return authors_case, authors


def _search_journal_words(item, title, journal):
    """Set if an item is a journal information by searching for specific words 
    using the regex 'RE_WOS_REF_JOURNAL', a global imported 
    from the `bpfuncts.regex_globals` module.

    The item is also kept as journal information if it is in upper case.
    Otherwise, the item is kept as title of the reference.

    Args:
        item (str): The item among the items resulting from the split \
        by ', ' of a reference.
        title (str): The previously kept item as title.
        journal (str): The previously kept item as journal information.
    Returns:
        (tup): Composed of the kept item as title (str) and the kept item \
        as journal information.
    """
    if re.findall(bp_rg.RE_WOS_REF_JOURNAL, item) or item.isupper():
        journal = item
    else:
        title = item
    return title, journal


def _find_wos_ref_title_journal(ref_items_list, authors_case, year):
    """Searches for the title and the journal information in the items resulting 
    from the split by ', ' of a reference through the `_search_journal_words` 
    internal function.

    Args:
        ref_items_list (list): The items (str) of the reference.
        authors_case (str): The case of the authors initials.
        year (str): The year kept for the reference.
    Returns:
        (tup): Composed of the kept item as title (str) and the kept item \
        as journal information.
    """
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
    """Builds the list of key items of the references of a publication as named-tuples.

    Args:
        pub_id (int): The publication ID.
        ref_field (str): The reference field giving the references of the publication.
        ref_cols_list (list): The column names to be used for the named-tuples.
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (list): The built named-tuples.
    """
    # Setting named-tuple for keeping the reference parsing results
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
        verbose (bool): True allows control prints (default: False).
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
