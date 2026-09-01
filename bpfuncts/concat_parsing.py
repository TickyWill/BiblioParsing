"""Module of functions for concatenation and deduplication 
of parsings results.
"""

__all__ = ['concatenate_parsing',
           'deduplicate_parsing']


# Standard libraries import
from difflib import SequenceMatcher

# 3rd party library imports
import numpy as np
import pandas as pd

# Local library imports
import bpfuncts.general_globals as bp_gg
import bpfuncts.parsing_cols_globals as bp_pcg
import bpfuncts.parsing_globals as bp_pg
from bpfuncts.affiliations_parsing import build_norm_and_raw_affils
from bpfuncts.affiliations_parsing import extend_author_affils


def _set_dedup_cols():
    """Builds a dict setting selected columns names for the process 
    of deduplicating parsing data.

    Returns:
        (dict): A dict valued by column names of parsing results.
    """
    cols_dic = {'pub_id_col'             : bp_pcg.COL_NAMES['pub_id'],
                'authors_col'            : bp_pcg.COL_NAMES['articles'][1],
                'page_col'               : bp_pcg.COL_NAMES['articles'][5],
                'doi_col'                : bp_pcg.COL_NAMES['articles'][6],
                'doc_type_col'           : bp_pcg.COL_NAMES['articles'][7],
                'title_col'              : bp_pcg.COL_NAMES['articles'][9],
                'issn_col'               : bp_pcg.COL_NAMES['articles'][10],
                'author_idx_col'         : bp_pcg.COL_NAMES['authors'][1],
                'address_idx_col'        : bp_pcg.COL_NAMES['address'][1],
                'country_addr_idx_col'   : bp_pcg.COL_NAMES['country'][1],
                'inst_addr_idx_col'      : bp_pcg.COL_NAMES['institution'][1],
                'auth_inst_auth_idx_col' : bp_pcg.COL_NAMES['auth_inst'][1],
                'lc_title_col'           : bp_pcg.COL_NAMES['temp_col'][0],
                'lc_doc_type_col'        : bp_pcg.COL_NAMES['temp_col'][5],
                'lc_doi_col'             : bp_pcg.COL_NAMES['temp_col'][6],
                'same_journal_col'       : bp_pcg.COL_NAMES['temp_col'][1],
                'norm_journal_col'       : bp_pcg.NORM_JOURNAL_COLUMN_LABEL,
               }
    return cols_dic


def _concatenate_item_dfs(item_first_corpus_df, item_second_corpus_df, pub_id_col):
    """Concatenates the parsing item's data of two corpuses referenced as first corpus 
    and second corpus.

    Args:
        item_first_corpus_df (dataframe): The parsing item's data of the first corpus.
        item_second_corpus_df (dataframe): The parsing item's data of the second corpus.
        pub_id_col (str): Name of the column of the publications identifiers.
    Returns:
        (dataframe): The item's concatenated data.
    """
    # Incrementing the "pub_id_col" column values of second corpus by first corpus length
    first_corpus_articles_nb = max(item_first_corpus_df[pub_id_col]) + 1
    new_item_second_corpus_df = item_second_corpus_df.copy()
    new_item_second_corpus_df[pub_id_col] = new_item_second_corpus_df[pub_id_col] + first_corpus_articles_nb

    # Concatenating the two dataframes
    dfs_list = [item_first_corpus_df, new_item_second_corpus_df]
    concat_df = pd.concat(dfs_list)
    concat_df.sort_values(by=[pub_id_col], inplace=True)

    return concat_df


def concatenate_parsing(first_parsing_dict, second_parsing_dict, affil_filter_list=None):
    """Concatenates parsing data of two corpuses using the `_concatenate_item_dfs` 
    internal function to the module.

    Then it proceeds with extending the "author with institutions" parsing data 
    using the `extend_author_affils` function imported from the 
    `biblioparsing.affilations_parsing` module.

    Args:
        first_parsing_dict (dict): The dict keyed by parsing items (str) and valued by data \
        resulting from the parsing of the first corpus (dataframe).
        second_parsing_dict (dict): The dict keyed by parsing items (str) and valued by data \
        resulting from the parsing of the second corpus (dataframe).
        affil_filter_list (list): Optional (default=None), the affiliations-filter composed of a list \
        of normalized affiliations (str).
    Returns:
        (dict): The dict keyed by parsing items (str) and valued by the concatenated parsing data (dataframe).
    """
    # Setting useful aliases
    pub_id_alias = bp_pcg.COL_NAMES['pub_id']
    auth_inst_item_alias = bp_pg.PARSING_ITEMS_LIST[5]

    # Getting a list of the common items of the parsing dicts
    first_items_set = set(first_parsing_dict.keys())
    second_items_set = set(second_parsing_dict.keys())
    common_items_list = list(first_items_set.intersection(second_items_set))

    # Concatenating the dicts of wos and scopus corpuses, item by item of the common_items_list
    concat_parsing_dict = {}
    for item in common_items_list:
        item_columns = list(first_parsing_dict[item].columns)
        if len(first_parsing_dict[item]) and len(second_parsing_dict[item]):
            concat_parsing_dict[item] = _concatenate_item_dfs(first_parsing_dict[item],
                                                              second_parsing_dict[item], pub_id_alias)
        elif len(second_parsing_dict[item]):
            concat_parsing_dict[item] = second_parsing_dict[item]
        elif len(first_parsing_dict[item]):
            concat_parsing_dict[item] = first_parsing_dict[item]
        else:
            concat_parsing_dict[item] = pd.DataFrame(columns=item_columns)

    # Extending the author with institutions parsing df
    if affil_filter_list and concat_parsing_dict[auth_inst_item_alias] is not None:
        return_df = extend_author_affils(concat_parsing_dict[auth_inst_item_alias],
                                         affil_filter_list)
        concat_parsing_dict[auth_inst_item_alias] = return_df
    return concat_parsing_dict


def _find_value_to_keep(dg, column_name, length_max=False):
    col_values_list = dg[column_name].to_list()
    col_values_list = list(dict.fromkeys(col_values_list))
    if bp_pg.UNKNOWN in col_values_list:
        col_values_list.remove(bp_pg.UNKNOWN)
    if length_max and len(col_values_list)>1:
        names_length_list = [len(x) for x in col_values_list]
        names_max_length = np.max(names_length_list)
        names_dict = dict(zip(col_values_list, names_length_list))
        longer_names_list = [name for name, name_length in names_dict.items()
                             if name_length==names_max_length]
        value_to_keep = longer_names_list[0]
    else:
        value_to_keep = col_values_list[0] if len(col_values_list)>0 else bp_pg.UNKNOWN
    return value_to_keep


def _norm_title(title):
    for init_symb, new_symb in bp_gg.TITLE_SYMB_CHANGE_DIC.items():
        title.replace(init_symb, new_symb)
    new_title = title.strip()
    return new_title


def _compute_similarity(a: str, b: str) -> int:
    similarity = round(SequenceMatcher(None, a, b).ratio() * 100)
    return similarity


def _set_same_journal_name(df, norm_journal_col, same_journal_col):
    print("      - Setting same journal names...")
    journals_list = df[norm_journal_col].to_list()
    journal_df = pd.DataFrame(journals_list, columns=[same_journal_col])
    lines_nb = len(journal_df)
    j1_idx = 0
    for j1 in journal_df[same_journal_col]:
        j1_idx += 1
        for j2 in journal_df[same_journal_col]:
            if j2!=j1 and (len(j1)>bp_pg.LENGTH_THRESHOLD and len(j2)>bp_pg.LENGTH_THRESHOLD):
                j1_set, j2_set = set(j1.split()), set(j2.split())
                common_words = j2_set.intersection(j1_set)
                j1_specific_words = j1_set - common_words
                j2_specific_words = j2_set - common_words
                similarity = _compute_similarity(j1, j2)
                if (similarity>bp_pg.SIMILARITY_THRESHOLD
                    or (j1_specific_words==set() or j2_specific_words==set())):
                    journal_df.loc[journal_df[same_journal_col]==j2] = j1
        print(f"            Number of journals checked: {j1_idx} / {lines_nb}", end="\r")
    df.reset_index(inplace=True, drop=True)
    same_journal_name_df = pd.concat([df, journal_df], axis=1)
    return same_journal_name_df


def _set_same_article_title(df, title_col, lc_title_col):
    print("      - Setting same publication's title...")
    titles_list = df[title_col].to_list()
    title_df = pd.DataFrame(titles_list, columns=[lc_title_col])
    lines_nb = len(title_df)
    t1_idx = 0
    for t1 in title_df[lc_title_col]:
        t1_idx += 1
        for t2 in title_df[lc_title_col]:
            if "part " not in t1 or "part " not in t2:
                if t2!=t1 and (len(t1)>bp_pg.LENGTH_THRESHOLD and len(t2)>bp_pg.LENGTH_THRESHOLD):
                    t1_set, t2_set = set(t1.split()), set(t2.split())
                    common_words = t2_set.intersection(t1_set)
                    t1_specific_words = t1_set - common_words
                    t2_specific_words = t2_set - common_words
                    similarity = _compute_similarity(t1, t2)
                    if (similarity>bp_pg.SIMILARITY_THRESHOLD
                        or (t1_specific_words==set() or t2_specific_words==set())):
                        title_df.loc[title_df[lc_title_col]==t2] = t1
            print(f"            Number of titles checked: {t1_idx}  / {lines_nb}", end="\r")
    title_df[lc_title_col] = title_df[lc_title_col].str.lower()
    title_df[lc_title_col] = title_df[lc_title_col].apply(_norm_title)
    df.reset_index(inplace=True, drop=True)
    same_title_df = pd.concat([df, title_df], axis=1)
    return same_title_df


def _set_issn(df, same_journal_col, issn_col):
    issn_df = df.copy()
    dfs_list = []
    for _, journal_dg in df.groupby(same_journal_col):
        if bp_pg.UNKNOWN in journal_dg[issn_col].to_list():
            journal_dg[issn_col] = _find_value_to_keep(journal_dg, issn_col)
        dfs_list.append(journal_dg)
    if dfs_list:
        issn_df = pd.concat(dfs_list)
    return issn_df


def _set_doi(df, lc_title_col, doi_col):
    doi_df = df.copy()
    dfs_list = []
    for _, title_dg in df.groupby(lc_title_col):
        if bp_pg.UNKNOWN in title_dg[doi_col].to_list():
            title_dg[doi_col] = _find_value_to_keep(title_dg, doi_col)
        dfs_list.append(title_dg)
    if dfs_list:
        doi_df = pd.concat(dfs_list)
    return doi_df


def _set_doc_type(df, doi_col, doc_type_col):
    doctype_df = df.copy()
    dfs_list = []
    for _, doi_dg in df.groupby(doi_col):
        if bp_pg.UNKNOWN in doi_dg[doc_type_col].to_list():
            doi_dg[doc_type_col] = _find_value_to_keep(doi_dg, doc_type_col)
        dfs_list.append(doi_dg)
    if dfs_list:
        doctype_df = pd.concat(dfs_list)
    return doctype_df


def _set_same_doi(df, cols_list):
    authors_col, lc_doc_type_col, issn_col, page_col, doi_col, lc_title_col, lc_doi_col = cols_list
    title_same_doi_df = df.copy()
    dfs_list = []
    for _, sub_df in df.groupby([authors_col, lc_doc_type_col, issn_col, page_col]):
        dois_list = sub_df[doi_col].to_list()
        titles_nb = len(list(set(sub_df[lc_title_col].to_list())))
        if titles_nb>1 and bp_pg.UNKNOWN in dois_list:
            sub_df[doi_col] = _find_value_to_keep(sub_df, doi_col)
            sub_df[lc_title_col] = _find_value_to_keep(sub_df, lc_title_col)
        dfs_list.append(sub_df)
    if dfs_list:
        title_same_doi_df = pd.concat(dfs_list)
    title_same_doi_df[lc_doi_col] = title_same_doi_df[doi_col].str.lower()
    return title_same_doi_df


def _set_same_first_author_name(df, cols_list):
    (lc_doc_type_col, issn_col, lc_title_col, page_col,
     pub_id_col, authors_col, lc_doi_col) = cols_list
    same_author_df = df.copy()
    dfs_list = []
    for _, sub_df in df.groupby([lc_doc_type_col, issn_col, lc_title_col, page_col]):
        authors_list = list(set(sub_df[authors_col].to_list()))
        lc_dois_list = list(set(sub_df[lc_doi_col].to_list()))
        authors_nb = len(authors_list)
        if authors_nb>1 and bp_pg.UNKNOWN in lc_dois_list:
            sub_df[authors_col] = _find_value_to_keep(sub_df, authors_col, length_max=True)
            sub_df[lc_doi_col] = _find_value_to_keep(sub_df, lc_doi_col)
        dfs_list.append(sub_df)
    if dfs_list:
        same_author_df = pd.concat(dfs_list)
    same_author_df.sort_values(by=[pub_id_col], inplace=True)
    return same_author_df


def _drop_duplicate_article1(df, cols_list):
    lc_doi_col, title_col, doc_type_col, lc_title_col, lc_doc_type_col = cols_list
    dfs_list = []
    for doi, dg in df.groupby(lc_doi_col):
        if doi!=bp_pg.UNKNOWN:
            # Deduplicating article lines by DOI
            dg[title_col]= _find_value_to_keep(dg, title_col)
            dg[doc_type_col] = _find_value_to_keep(dg, doc_type_col)
            dg.drop_duplicates(subset=[lc_doi_col], keep='first', inplace=True)
        else:
            # Deduplicating article lines without DOI by title and document type
            dg.drop_duplicates(subset=[lc_title_col, lc_doc_type_col], keep='first', inplace=True)
        dfs_list.append(dg)
    doi_dedup_df = pd.concat(dfs_list)
    return doi_dedup_df


def _drop_duplicate_article2(df, cols_list):
    lc_title_col, lc_doc_type_col, same_journal_col, lc_doi_col, pub_id_col = cols_list
    dedup_df = df.copy()
    dfs_list = []
    for same_list, dg in df.groupby([lc_title_col, lc_doc_type_col, same_journal_col]):
        new_dg = dg.copy()
        if len(new_dg)>1:
            # Dropping publications data with DOI bp_pg.UNKNOWN from group of publications with same title,
            # document type, first author and journal
            unknown_doi_idx = dg[dg[lc_doi_col]==bp_pg.UNKNOWN].index
            dg_wo_unknown_doi = dg.drop(unknown_doi_idx)
            new_dg = dg_wo_unknown_doi.drop_duplicates(subset=[lc_doi_col], keep='first')
            if len(new_dg)>1:
                # Warning that publications with same title, document type, first author and journal have different DOIs
                pub_ids_list = list(new_dg[pub_id_col])
                warning = ('           - WARNING: Multiple DOI values for same title, document type, first author and journal '
                           f"for the publications' with IDs: {pub_ids_list}")
                print(warning)
        dfs_list.append(new_dg)
    if dfs_list:
        dedup_df = pd.concat(dfs_list)
    dedup_df = dedup_df.drop([lc_title_col, lc_doc_type_col, lc_doi_col], axis=1)
    dedup_df.sort_values(by=[pub_id_col], inplace=True)
    return dedup_df


def _norm_doctype(doctype):
    # Normalizing document type
    lc_doctype = doctype.lower()
    norm_doctype = lc_doctype
    for key, values in bp_pg.LC_DOCTYPE_DIC.items():
        if lc_doctype in values:
            norm_doctype = key
    return norm_doctype


def _deduplicate_articles(init_articles_concat_df, cols_dic, verbose=False):
    """Uses the concatenated publications list and applies a succesion of filters
    to get rid of duplicated information.

    Args:
        init_articles_concat_df (dataframe) : The concatenated selected data of the publications.
        cols_dic (dict): Columns information as built through the `_set_dedup_cols` \
        internal function.
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (list): the list contains a dataframe of articles with no duplicates but unfull information, 
                a list of dataframes each of them containing a line that is a duplicate in the articles dataframe,
                and a list of the duplicate indices.
    """
    print("  - Deduplicating publications main data...")

    # Setting useful column names
    cols_keys = ['pub_id_col', 'authors_col', 'page_col', 'doi_col',
                 'doc_type_col', 'title_col', 'issn_col',
                 'lc_title_col', 'lc_doc_type_col', 'lc_doi_col',
                 'norm_journal_col', 'same_journal_col']
    (pub_id_col, authors_col, page_col, doi_col,
     doc_type_col, title_col, issn_col,
     lc_title_col, lc_doc_type_col, lc_doi_col,
     norm_journal_col, same_journal_col) = [cols_dic[key] for key in cols_keys]

    # Setting same journal name for similar journal names
    inter1_articles_concat_df = _set_same_journal_name(init_articles_concat_df, norm_journal_col,
                                                       same_journal_col)
    print("      - Column with unique journal name added to the publications data")

    # Setting same article title for similar article title
    inter2_articles_concat_df = _set_same_article_title(inter1_articles_concat_df, title_col,
                                                        lc_title_col)
    print("      - Titles of publications standardized                        ")

    # Setting issn when unknown for given article ID using available issn values
    # of journals of same normalized names from other article IDs
    issn_articles_concat_df = _set_issn(inter2_articles_concat_df, same_journal_col, issn_col)
    print("      - Available ISSN value set common to journals with same name")

    # Adding useful temporal columns
    issn_articles_concat_df[lc_title_col] = issn_articles_concat_df[lc_title_col].str.lower()
    issn_articles_concat_df[lc_doc_type_col] = issn_articles_concat_df[doc_type_col].apply(_norm_doctype)
    issn_articles_concat_df[title_col] = issn_articles_concat_df[title_col].str.strip()

    # Setting DOI when unknown for given article ID using available DOI values
    # of articles of same title from other article IDs
    # Modification on 09-2023
    doi_articles_concat_df = _set_doi(issn_articles_concat_df, lc_title_col, doi_col)
    print("      - Available DOI value set common to publications with same title")

    # Setting document type when unknown for given article ID using available document type values
    # of articles of same DOI from other article IDs
    # Modification on 09-2023
    doctype_articles_concat_df = _set_doc_type(doi_articles_concat_df, doi_col, doc_type_col)
    print("      - Available document-type value set common to publications with same DOI")

    # Setting same DOI for similar titles when any DOI is unknown
    # for same first author, page, document type and ISSN
    # Modification on 09-2023
    cols_list = [authors_col, lc_doc_type_col, issn_col, page_col, doi_col, lc_title_col, lc_doi_col]
    title_articles_concat_df = _set_same_doi(doctype_articles_concat_df, cols_list)
    print("      - Available DOI value set common to publications "
          "with same first author, page, document type and ISSN")

    # Setting same first author name for same page, document type and ISSN
    # when DOI is unknown or DOIs are different
    # Modification on 09-2023
    cols_list = [lc_doc_type_col, issn_col, lc_title_col, page_col,
                 pub_id_col, authors_col, lc_doi_col]
    author_articles_concat_df = _set_same_first_author_name(title_articles_concat_df, cols_list)
    print("      - Same first author name set common to publications with same page, document type and ISSN")

    # Keeping copy of author_articles_concat_df with 'same_journal_col', 'issn_col', 'doi_col'
    # and 'doc_type_col' columns completed
    full_articles_concat_df = author_articles_concat_df.copy()

    # Dropping duplicated publication data after merging by doi or, for unknown doi, by title and document type
    cols_list = [lc_doi_col, title_col, doc_type_col, lc_title_col, lc_doc_type_col]
    doi_articles_dedup_df = _drop_duplicate_article1(author_articles_concat_df, cols_list)
    print("      - Publication data with same DOI deduplicated on DOI except for unknown DOI")
    print("      - Publication data with unknown DOI deduplicated on title and document type")

    # Dropping duplicated publication data after merging by title, document type and journal
    cols_list = [lc_title_col, lc_doc_type_col, same_journal_col, lc_doi_col, pub_id_col]
    articles_dedup_df = _drop_duplicate_article2(doi_articles_dedup_df, cols_list)
    print("      - Publication data deduplicated on title, document type and journal")

    # Identifying the set of articles IDs to drop in the other parsing files of the concatenated corpus
    pub_id_set_init = set(full_articles_concat_df[pub_id_col].to_list())
    pub_id_set_end  = set(articles_dedup_df[pub_id_col].to_list())
    pub_id_to_drop  = pub_id_set_init - pub_id_set_end
    print("      - List of publication identifiers to drop in other concatenated parsing data built")

    # Setting usefull prints
    articles_nb_init = len(full_articles_concat_df)
    articles_nb_end  = len(articles_dedup_df)
    articles_nb_drop = articles_nb_init - articles_nb_end

    if verbose:
        print('\nDeduplication results:')
        print(f'    Initial publications number: {articles_nb_init}')
        print(f'    Final publications number: {articles_nb_end}')
        warning = f'    WARNING: {articles_nb_drop} publications have been dropped as duplicates'
        print(warning)

    return articles_dedup_df, pub_id_to_drop


def _deduplicate_item_df(pub_ids_to_drop, item_df, pub_id_col, second_col):
    """Drops the item's data corresponding to the publication identifiers of the passed list of identifiers.

    Args:
       pub_ids_to_drop (list): The list of pubblication identifiers which data should be dropped.
       item_df (df): The item data targetted by the deduplication process.
       pub_id_col (str): The column name that contains the publication identifiers in the item's data.
       second_col (str): The possible name of the second column used to sort the deduplicated data.
    Returns:
       (dataframe): The deduplicated data of the item.
    """
    # Selecting item's data to keep
    filt = item_df[pub_id_col].isin(pub_ids_to_drop)
    item_dg = item_df[~filt].copy()
    item_dg.sort_values(by=[pub_id_col], inplace=True)

    if second_col:
        item_dg.sort_values(by=[pub_id_col, second_col], inplace=True)
    return item_dg


def deduplicate_parsing(concat_parsing_dict, norm_affil_status=False, affil_params_dic=None, verbose=False):
    """Deduplicates parsing data from the concatenated parsing data.

    It proceeds with deduplication of publications data using the `_deduplicate_articles` internal 
    function of the module. 
    Then, it rationalizes the content of the other parsing data using the publication identifiers 
    of the droped publications data using the `_deduplicate_item_df` internal function of the module. 
    The outputs are the deduplicated parsing data of the corpus.

    Args:
        concat_parsing_dict (dict): Dict with keys as items parsing (str) and values (dataframe) as \
        the data resulting from the concatenation of corpuses parsings.
        norm_affil_status (bool): If true (dafault= False), normalized institutions and of not-yet \
        normalized institutions are built.
        affil_params_dic (dict): Optional dict (default=None) keyed by ['affil_types_file_path', \
        'country_affils_file_path', 'country_towns_folder_path', 'country_towns_file'] and valued \
        by the user as the full path to the data per country of raw affiliations per normalized one, \
        the full path to the data of affiliations-types used to normalize the affiliations, \
        the name of the file of the data of towns per country and the full path to the folder \
        where these data are available.
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (dict): The dict keyed by parsing items (str) and valued by deduplicated parsing data(dataframe).
    """
    # Setting useful col names
    cols_dic = _set_dedup_cols()
    cols_keys = ['pub_id_col', 'author_idx_col', 'address_idx_col', 'country_addr_idx_col',
                 'inst_addr_idx_col', 'auth_inst_auth_idx_col']
    cols_list = [cols_dic[key] for key in cols_keys]
    pub_id_col = cols_list[0]

    # Setting second cols for sorting item's data after deduplication for selected items
    second_col_items_list = [bp_pg.PARSING_ITEMS_LIST[x] for x in range(1, 6)]
    sorting_second_col_dict = dict(zip(second_col_items_list, cols_list[1:]))

    # Setting useful items' lists and items' values for deduplication process
    full_items_list = list(concat_parsing_dict.keys())
    sub_items_list = [bp_pg.PARSING_ITEMS_LIST[x] for x in [0, 2, 12, 13]]
    articles_item, addresses_item, norm_inst_item, raw_inst_item = sub_items_list
    items_list_wo_articles = full_items_list.copy()
    items_list_wo_articles.remove(articles_item)

    # Building deduplicated data per item
    dedup_parsing_dict = {}
    concat_articles_df = concat_parsing_dict[articles_item]
    articles_dedup_df, pub_ids_to_drop = _deduplicate_articles(concat_articles_df, cols_dic)
    dedup_parsing_dict[articles_item] = articles_dedup_df
    for item in items_list_wo_articles:
        item_df = concat_parsing_dict[item]
        second_col = ""
        if item in second_col_items_list:
            second_col = sorting_second_col_dict[item]
        dedup_parsing_dict[item] = _deduplicate_item_df(pub_ids_to_drop, item_df, pub_id_col, second_col)

    if norm_affil_status:
        # Creating data of normalized institutions and of not-yet normalized institutions
        address_df = dedup_parsing_dict[addresses_item]
        return_tup = build_norm_and_raw_affils(address_df, affil_params_dic=affil_params_dic,
                                               verbose=verbose)
        _, norm_institution_df, raw_institution_df, _ = return_tup
        dedup_parsing_dict[norm_inst_item] = norm_institution_df
        dedup_parsing_dict[raw_inst_item] = raw_institution_df
    return dedup_parsing_dict
