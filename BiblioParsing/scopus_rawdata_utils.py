"""Module of functions for reading and cleaning of Scopus rawdata.
"""

__all__ = ['read_scopus_rawdata']


# Standard library imports
import re

# 3rd party library imports
import numpy as np
import pandas as pd

# Local libray imports
import BiblioParsing.parsing_cols_globals as bp_pcg
import BiblioParsing.parsing_globals as bp_pg
import BiblioParsing.regex_globals as bp_rg
from BiblioParsing.parsing_utils import build_pub_db_ids
from BiblioParsing.parsing_utils import check_and_drop_columns
from BiblioParsing.parsing_utils import check_and_get_rawdata_file_path
from BiblioParsing.parsing_utils import drop_rawdata
from BiblioParsing.parsing_utils import normalize_country
from BiblioParsing.parsing_utils import normalize_journal_names
from BiblioParsing.parsing_utils import normalize_name
from BiblioParsing.parsing_utils import remove_special_symbol
from BiblioParsing.parsing_utils import standardize_str


def _set_scopus_rawdata_cols():
    """Builds 2 dict setting selected columns names for the process of getting 
    and cleaning Scopus rawdata.

    Returns:
        (tup): (A dict valued by column names of parsing results defined by the \
        'COL_NAMES' global, A dict valued by column names of rawdata defined \
        by the 'COLUMN_LABEL_SCOPUS' and 'COLUMN_LABEL_SCOPUS_PLUS' globals).
    """
    cols_dic = {'scopus_id_col'           : bp_pcg.COL_NAMES['scopus_id'][0],
                'pub_id_col'              : bp_pcg.COL_NAMES['pub_id'],
               }

    scopus_cols_dic = {'scopus_auth_col'         : bp_pcg.COLUMN_LABEL_SCOPUS['authors'],
                       'scopus_aff_col'          : bp_pcg.COLUMN_LABEL_SCOPUS['affiliations'],
                       'scopus_auth_with_aff_col': bp_pcg.COLUMN_LABEL_SCOPUS['authors_with_affiliations'],
                       'scopus_fullnames_col'    : bp_pcg.COLUMN_LABEL_SCOPUS_PLUS['auth_fullnames'],
                       'init_scopus_id_col'      : bp_pcg.COLUMN_LABEL_SCOPUS_PLUS['scopus_id'],
                      }

    return cols_dic, scopus_cols_dic


def _check_authors_with_affiliations(corpus_df, check_cols):
    """Corrects the list of affiliations and the list of authors-with-affiliations 
    when irregular sequence of separators induces a discrepancy between number 
    of authors and number of authors-with-affiliations.

    Args:
        corpus_df (dataframe): The full rawdata of the corpus.
        check_cols (list): The column names where the authors \
        names or affiliations are present.
    Returns:
        (tup): (The corrected full rawdata of the corpus (dataframe), \
        The data (dataframe) of corrected affiliations).
    """
    pub_id_col, authors_col, affil_col, auth_affil_col = check_cols
    corrected_addresses_data = []
    corrected_corpus_df = corpus_df.copy()
    for row_idx, row in corpus_df.iterrows():
        pub_id = row[pub_id_col]
        init_authors_str = row[authors_col]
        init_affil_str = row[affil_col]
        init_auth_affil_str = row[auth_affil_col]

        std_sep = "; "
        authors_list = init_authors_str.split(std_sep)
        affil_list = init_affil_str.split(std_sep)
        auth_affil_list = init_auth_affil_str.split(std_sep)

        check_sep = ";"
        check_auth_affil_list = init_auth_affil_str.split(check_sep)

        authors_nb = len(authors_list)
        auth_affil_nb = len(check_auth_affil_list)
        if authors_nb!=auth_affil_nb:
            authors_status, affil_status, auth_affil_status = 0, 0, 0
            auth_false_sep, auth_correct_sep= ";", ""
            if any(auth_false_sep in s for s in authors_list):
                correct_authors_list = [x.replace(auth_false_sep, auth_correct_sep) for x in authors_list]
                new_auth_affil_list = [x.replace(auth_false_sep, auth_correct_sep) for x in auth_affil_list]
                authors_status = 1
            else:
                correct_authors_list = authors_list
                new_auth_affil_list = auth_affil_list

            if any(re.search(bp_rg.RE_AWA, s) for s in affil_list):
                correct_affil_list = []
                for affil in affil_list:
                    new_affil = ", ".join(affil.split(";, "))
                    new_affil = ", ".join(new_affil.split(";"))
                    correct_affil_list.append(new_affil)
                affil_status = 1
            else:
                correct_affil_list = affil_list

            if any(re.search(bp_rg.RE_AWA, s) for s in new_auth_affil_list):
                correct_auth_affil_list = []
                for auth_affil in new_auth_affil_list:
                    new_auth_affil = ", ".join(auth_affil.split(";, "))
                    new_auth_affil = ", ".join(new_auth_affil.split(";"))
                    correct_auth_affil_list.append(new_auth_affil)
                auth_affil_status = 1
            else:
                correct_auth_affil_list = new_auth_affil_list

            correct_authors_str = std_sep.join(correct_authors_list)
            correct_affil_str = std_sep.join(correct_affil_list)
            correct_auth_affil_str = std_sep.join(correct_auth_affil_list)
            corrected_addresses_data.append([pub_id, authors_status, affil_status, auth_affil_status,
                                             init_authors_str, correct_authors_str,
                                             init_affil_str, correct_affil_str,
                                             init_auth_affil_str, correct_auth_affil_str])
        else:
            correct_authors_str = init_authors_str
            correct_affil_str = init_affil_str
            correct_auth_affil_str = init_auth_affil_str

        # Updating corpus data
        corrected_corpus_df.loc[row_idx, authors_col] = correct_authors_str
        corrected_corpus_df.loc[row_idx, affil_col] = correct_affil_str
        corrected_corpus_df.loc[row_idx, auth_affil_col] = correct_auth_affil_str

    correction_cols = [pub_id_col, "Authors status", "Affiliations status", "Auth with affil status",
                       authors_col, "Corrected " + authors_col,
                       affil_col, "Corrected " + affil_col,
                       auth_affil_col, "Corrected " + auth_affil_col]
    corrected_addresses_df = pd.DataFrame(corrected_addresses_data, columns=correction_cols)
    return corrected_corpus_df, corrected_addresses_df


def _correct_firstname_initials(fullname_init):
    fullname = fullname_init
    # Remove author digital identifier
    if "(" in fullname_init:
        fullname = fullname_init.split(" (")[0]
    if ',' in fullname:
        lastname, firstname = fullname.split(", ")
    else:
        # Assuming a team as author
        lastname, firstname = fullname, "Unknown First Name"

    # Normalizing author's last-name with punctuation drop (specifically ";")
    lastname = normalize_name(lastname, drop_ponct=True, lastname_only=True)

    # Normalizing author's first name keeping punctuation (specifically ".")
    firstname = normalize_name(firstname, drop_ponct=False, firstname_only=True)

    # Building firstname initials
    firstname = firstname.replace('-',' ').strip(' ')
    firstname_list = sum([x.split('.') for x in firstname.split(' ')], [])
    initials_list = [x[0] + "." for x in firstname_list if x]
    initials = ''.join(initials_list)

    # Building new author name
    new_author = ' '.join([lastname, initials])
    return new_author


def _correct_auth_data(auth_tup):
    fullname, auth_affil = auth_tup

    # Correcting author name
    new_author = _correct_firstname_initials(fullname)

    # Updating author-with-affiliations with the corrected author name
    auth_affil_split = auth_affil.split(", ")
    affil = ", ".join(auth_affil_split[1:])
    new_auth_affil = ", ".join([new_author, affil])
    return new_author, new_auth_affil


def _check_authors(corpus_df, check_cols):
    """Corrects the firstname initials for the authors using 
    the full names given in the full corpus data.

    Args:
        corpus_df (dataframe): The full rawdata of the corpus.
        check_cols (list): The column names where the authors \
        names are present.
    Returns:
        (tup): (The corrected full rawdata of the corpus (dataframe), \
        The data (dataframe) of corrected authors).
    """
    pub_id_col, authors_col, fullname_col, affil_col, auth_affil_col = check_cols
    corrected_authors_data = []
    new_corpus_df = corpus_df.copy()
    for row_idx, row in corpus_df.iterrows():
        pub_id = row[pub_id_col]

        # Removing accentuated characters
        authors_str = remove_special_symbol(row[authors_col])
        fullnames_str = remove_special_symbol(row[fullname_col])
        auth_affil_str = standardize_str(row[auth_affil_col])
        aff_str = standardize_str(row[affil_col])

        # Building dict keyed by author and valued by a tuple
        # composed of fullname and author-with-affiliations
        authors_list = authors_str.split("; ")
        fullnames_list = fullnames_str.split("; ")
        auth_affil_list = auth_affil_str.split("; ")
        author_tup_list = list(zip(fullnames_list, auth_affil_list))
        auth_data_dict = dict(zip(authors_list, author_tup_list))

        # Correcting list of authors and list of authors-with-affiliations
        new_authors_list = []
        new_auth_affils_list = []
        for author, auth_tup in auth_data_dict.items():
            new_author, new_auth_affil = _correct_auth_data(auth_tup)
            if author!=new_author:
                corrected_authors_data.append([pub_id, author, new_author])
            new_authors_list.append(new_author)
            new_auth_affils_list.append(new_auth_affil)

        # Updating the corpus data with the corrected lists
        new_corpus_df.loc[row_idx, authors_col] = "; ".join(new_authors_list)
        new_corpus_df.loc[row_idx, auth_affil_col] = "; ".join(new_auth_affils_list)
        new_corpus_df.loc[row_idx, affil_col] = aff_str
    correction_cols = [pub_id_col, authors_col, "Corrected " + authors_col]
    corrected_authors_df = pd.DataFrame(corrected_authors_data, columns=correction_cols)
    return new_corpus_df, corrected_authors_df


def _correct_scopus_full_rawdata(corpus_df, cols_tup):
    """Corrects firstname initials and affiliations of authors 
    in the full rawdata of the corpus.

    Args:
        corpus_df (dataframe): The full rawdata of the corpus.
        cols_tup (tup): Columns information as built through \
        the `_set_scopus_parsing_cols` internal function.
    Returns:
        (dataframe): The corrected full rawdata of the corpus.
    """
    # Setting useful column names
    cols_dic, scopus_cols_dic = cols_tup
    pub_id_col = cols_dic['pub_id_col']
    scopus_cols_keys = ['scopus_auth_col','scopus_aff_col', 'scopus_auth_with_aff_col',
                        'scopus_fullnames_col']
    (scopus_auth_col, scopus_aff_col, scopus_auth_with_aff_col,
     scopus_fullnames_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]

    affil_check_cols = [pub_id_col, scopus_auth_col,
                        scopus_aff_col, scopus_auth_with_aff_col]
    auth_check_cols = [pub_id_col, scopus_auth_col, scopus_fullnames_col,
                       scopus_aff_col, scopus_auth_with_aff_col]

    # Setting the pub_id in df index
    corpus_df.index = range(len(corpus_df))

    # Setting the pub-id as a column
    corpus_df = corpus_df.rename_axis(pub_id_col).reset_index()

    # Correcting corpus data
    new_corpus_df, corrected_addresses_df = _check_authors_with_affiliations(corpus_df, affil_check_cols)
    new_corpus_df, corrected_authors_df = _check_authors(new_corpus_df, auth_check_cols)

    # Dropping pub_id_col column
    new_corpus_df.drop(columns=[pub_id_col], inplace=True)
    return new_corpus_df, corrected_authors_df, corrected_addresses_df


def _check_scopus_affiliation_column(df, scopus_aff_col):
    """Checks the correctness of the column affiliation of data read from a csv scopus file.

    A cell of the column affiliation should read:
    address<0>, country<0>;...; address<i>, country<i>;...

    Some cells can be misformatted with an incorrect country field. The function eliminates, for each
    cell of the column, those items address<i>, country<i> incorrectly formatted. When such an item is detected
    a warning message is printed.
    """
    #To Do: Doc string update
    def _valid_affiliation(affiliations_str):
        nonlocal idx
        idx += 1
        valid_affiliation_list = []
        for affiliation in affiliations_str.split('; '):
            raw_country = affiliation.split(', ')[-1].strip()
            if normalize_country(raw_country):
                valid_affiliation_list.append(affiliation)
            else:
                warning = ('\nWARNING in "_check_scopus_affiliation_column" function "'
                           '"of "scopus_rawdata_utils.py" module:'
                           f'\nAt row {idx} of the scopus corpus, the invalid affiliation "{affiliation}" '
                           'has been dropped from the list of affiliations. '
                           '\nTherefore, attention should be given to the resulting list of affiliations '
                           'for each of the authors of this publication.\n' )
                print(warning)
        new_affiliations_str = bp_pg.UNKNOWN
        if  valid_affiliation_list:
            new_affiliations_str = '; '.join(valid_affiliation_list)
        return new_affiliations_str

    idx = -1
    df[scopus_aff_col] = df[scopus_aff_col].apply(_valid_affiliation)
    return df


def read_scopus_rawdata(rawdata_path, correct_data=False, scopus_ids=False):
    """Reads the file of Scopus rawdata available in the indicated folder.

    First, it can corrects the first name initials and the affiliations
    of the authors when required using the `_correct_scopus_full_rawdata` 
    internal function. 
    Then, the function:
    - Checks columns and drops unuseful columns using the \
    `check_and_drop_columns` function imported from `BiblioParsingUtils` module.
    - Checks the affiliation column content using the `_check_scopus_affiliation_column` \
    internal function.
    - Replaces the unavailable items values by a string set in the global UNKNOWN.
    - Normalizes the journal names using the `normalize_journal_names` function \
    imported from the `BiblioParsingUtils` module.
    Finally, the function can built data of Scopus identifiers of the publications.
    The returned data are initialized to empty dataframes.

    Args:
        rawdata_path (path): The full path to the Scopus-rawdata file.
        correct_data (bool): Optional, true for correcting authors' names \
        and addresses (default=False).
        scopus_ids (bool): Optional (default=False), True for building the data \
        of Scopus IDs of publications .
    Returns:
        (tup): (The cleaned corpus data (dataframe), The optional data of corrected \
        authors' names (dataframe), The optional data of corrected addresses (dataframe), \
        The optional Scopus-IDs data (dataframe)).
    """
    # Setting columns for scopus parsing process
    cols_tup = _set_scopus_rawdata_cols()
    cols_dic, scopus_cols_dic = cols_tup
    scopus_id_col = cols_dic['scopus_id_col']
    scopus_cols_keys = ['init_scopus_id_col', 'scopus_aff_col']
    (init_scopus_id_col, scopus_aff_col) = [scopus_cols_dic[key] for key in scopus_cols_keys]
    scopus_ids_cols_list = [scopus_id_col, init_scopus_id_col]

    # Initializing returned data to empty dataframes
    scopus_rawdata_df = pd.DataFrame()
    corrected_authors_df = pd.DataFrame()
    corrected_addresses_df = pd.DataFrame()
    scopus_ids_df = pd.DataFrame()

    # Check if rawdata file is available and get its full path if it is
    rawdata_file_path = check_and_get_rawdata_file_path(rawdata_path, bp_pg.SCOPUS_RAWDATA_EXTENT)

    if rawdata_file_path:
        init_full_scopus_rawdata_df = pd.read_csv(rawdata_file_path, dtype=bp_pcg.COLUMN_TYPE_SCOPUS)

        if len(init_full_scopus_rawdata_df):
            # Trying to drop data by scopus identifier given in an XLSX file
            full_scopus_rawdata_df = drop_rawdata(rawdata_path, init_full_scopus_rawdata_df,
                                                  scopus_ids_cols_list, bp_pg.SCOPUS)

            if correct_data:
                return_tup = _correct_scopus_full_rawdata(full_scopus_rawdata_df, cols_tup)
                full_scopus_rawdata_df, corrected_authors_df, corrected_addresses_df = return_tup

            # Selecting useful rawdata for parsing
            scopus_rawdata_df = check_and_drop_columns(bp_pg.SCOPUS, full_scopus_rawdata_df)
            scopus_rawdata_df = _check_scopus_affiliation_column(scopus_rawdata_df, scopus_aff_col)
            scopus_rawdata_df = scopus_rawdata_df.replace(np.nan, bp_pg.UNKNOWN, regex=True)
            scopus_rawdata_df = normalize_journal_names(bp_pg.SCOPUS, scopus_rawdata_df)

            if scopus_ids:
                # Building the Scopus-IDs data
                scopus_ids_df = build_pub_db_ids(full_scopus_rawdata_df, init_scopus_id_col, scopus_id_col)
    return_tup = (scopus_rawdata_df, corrected_authors_df, corrected_addresses_df, scopus_ids_df)
    return return_tup
