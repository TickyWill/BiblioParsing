__all__ = ['concatenate_parsing', 
           'deduplicate_parsing']


# Standard libraries import
import numpy as np
from pathlib import Path
from difflib import SequenceMatcher

# 3rd party library imports
import pandas as pd

# Local library imports
import BiblioParsing.BiblioGeneralGlobals as bp_gg
import BiblioParsing.BiblioSpecificGlobals as bp_sg
from BiblioParsing.BiblioParsingInstitutions import build_norm_raw_institutions
from BiblioParsing.BiblioParsingInstitutions import extend_author_institutions
from BiblioParsing.BiblioParsingUtils import dict_print


def _set_dedup_cols():
    """Builds a dict setting selected columns names for the process 
    of deduplicating parsing data.

    Returns:
        (dict): A dict valued by column names of parsing results.
    """
    cols_dic = {'pub_id_col'             : bp_sg.COL_NAMES['pub_id'],
                'authors_col'            : bp_sg.COL_NAMES['articles'][1],
                'page_col'               : bp_sg.COL_NAMES['articles'][5],
                'doi_col'                : bp_sg.COL_NAMES['articles'][6],
                'doc_type_col'           : bp_sg.COL_NAMES['articles'][7], 
                'title_col'              : bp_sg.COL_NAMES['articles'][9],
                'issn_col'               : bp_sg.COL_NAMES['articles'][10],
                'author_idx_col'         : bp_sg.COL_NAMES['authors'][1],
                'address_idx_col'        : bp_sg.COL_NAMES['address'][1],
                'country_addr_idx_col'   : bp_sg.COL_NAMES['country'][1],
                'inst_addr_idx_col'      : bp_sg.COL_NAMES['institution'][1],
                'auth_inst_auth_idx_col' : bp_sg.COL_NAMES['auth_inst'][1],
                'lc_title_col'           : bp_sg.COL_NAMES['temp_col'][0], 
                'lc_doc_type_col'        : bp_sg.COL_NAMES['temp_col'][5],
                'lc_doi_col'             : bp_sg.COL_NAMES['temp_col'][6],
                'same_journal_col'       : bp_sg.COL_NAMES['temp_col'][1],
                'norm_journal_col'       : bp_sg.NORM_JOURNAL_COLUMN_LABEL,
               }
    return cols_dic


def _concatenate_item_dfs(item, first_corpus_df, second_corpus_df, pub_id_col):
    """Concatenates the parsing item's data of two corpuses referenced as first corpus 
    and second corpus.

    Args:
        item (string): The parsing item's name.
        first_corpus_df (dataframe): The parsing item's data of the first corpus.
        second_corpus_df (dataframe): The parsing item's data of the second corpus.
        pub_id_col (str): Name of the column of the publications identifiers.
    Returns:
        (dataframe): The item's concatenated data.
    """
    # Incrementing the "pub_id_col" column values of second corpus by first corpus length 
    first_corpus_articles_nb = max(first_corpus_df[pub_id_col]) + 1
    new_second_corpus_df = second_corpus_df.copy()
    new_second_corpus_df[pub_id_col] = new_second_corpus_df[pub_id_col] + first_corpus_articles_nb

    # Concatenating the two dataframes
    dfs_list = [first_corpus_df, new_second_corpus_df]
    concat_df = pd.concat(dfs_list)
    concat_df.sort_values(by=[pub_id_col], inplace=True)

    return concat_df


def concatenate_parsing(first_parsing_dict, second_parsing_dict, inst_filter_list=None):
    """Concatenates parsing dfs of two corpuses using the `_concatenate_item_dfs` 
    internal function to the module. 

    Then it proceeds with extending the "author with institutions" parsing data 
    using the `extend_author_institutions` function. 
    The outputs are the concatenated parsing data of the corpus.

    Args:
        first_parsing_dict (dict): Dict with keys as items parsing and values as the dfs 
                                   resulting from the parsing of the first corpus.
        second_parsing_dict (dict): Dict with keys as items parsing and values as the dfs 
                                    resulting from the parsing of the second corpus.
        inst_filter_list (list): The affiliations-filter composed of a list of normalized \
        affiliations (str), optional (default=None).
    Returns: 
        (dict): Dict with keys as parsing items (str) and values (dataframe) as \
        the concatenated data.
    """
    # Setting useful aliases
    pub_id_alias = bp_sg.COL_NAMES['pub_id']
    auth_inst_item_alias = bp_sg.PARSING_ITEMS_LIST[5]

    # Getting a list of the common items of the parsing dicts
    first_items_set = set(first_parsing_dict.keys())
    second_items_set = set(second_parsing_dict.keys())
    common_items_list = list(first_items_set.intersection(second_items_set))

    # Concatenating the dicts of wos and scopus corpuses, item by item of the common_items_list
    concat_parsing_dict = {}
    for item in common_items_list:
        if len(first_parsing_dict[item]) and len(second_parsing_dict[item]):
            concat_parsing_dict[item] = _concatenate_item_dfs(item, first_parsing_dict[item],
                                                              second_parsing_dict[item], pub_id_alias)
        elif len(second_parsing_dict[item]):
            concat_parsing_dict[item] = second_parsing_dict[item]
        elif len(first_parsing_dict[item]):
            concat_parsing_dict[item] = first_parsing_dict[item]
        else:
            concat_parsing_dict[item] = None

    # Extending the author with institutions parsing df
    if inst_filter_list and concat_parsing_dict[auth_inst_item_alias] is not None:
        concat_parsing_dict[auth_inst_item_alias] = extend_author_institutions(concat_parsing_dict[auth_inst_item_alias],
                                                                               inst_filter_list)
    return concat_parsing_dict


def _find_value_to_keep(dg, column_name, length_max=False):
    col_values_list = dg[column_name].to_list()
    col_values_list = list(dict.fromkeys(col_values_list)) 
    if bp_sg.UNKNOWN in col_values_list:
        col_values_list.remove(bp_sg.UNKNOWN)
    if length_max and len(col_values_list)>1:
        names_length_list = [len(x) for x in col_values_list]
        names_max_length = np.max(names_length_list)
        names_dict = dict(zip(col_values_list, names_length_list))
        longer_names_list = [name for name in names_dict.keys() if names_dict[name]==names_max_length]
        value_to_keep = longer_names_list[0]
    else:
        value_to_keep = col_values_list[0] if len(col_values_list)>0 else bp_sg.UNKNOWN
    return value_to_keep


def _setting_same_journal_name(df, norm_journal_col, same_journal_col, similar):
    print("    Setting same journal names...")
    journals_list = df[norm_journal_col].to_list()
    journal_df = pd.DataFrame(journals_list, columns=[same_journal_col])
    lines_nb = len(journal_df)
    j1_idx = 0
    for j1 in journal_df[same_journal_col]:
        j1_idx += 1
        for j2 in journal_df[same_journal_col]:
            if j2!=j1 and (len(j1)>bp_sg.LENGTH_THRESHOLD and len(j2)>bp_sg.LENGTH_THRESHOLD):
                j1_set, j2_set = set(j1.split()), set(j2.split())
                common_words = j2_set.intersection(j1_set)
                j1_specific_words, j2_specific_words = (j1_set - common_words), (j2_set - common_words)
                similarity = round(similar(j1, j2)*100)    
                if (similarity>bp_sg.SIMILARITY_THRESHOLD) or (j1_specific_words==set() or j2_specific_words==set()):
                    journal_df.loc[journal_df[same_journal_col]==j2] = j1
        print(f"        Number of journals searched: {j1_idx} / {lines_nb}", end="\r")
    df.reset_index(inplace=True, drop=True)
    same_journal_name_df = pd.concat([df, journal_df], axis=1)
    return same_journal_name_df


def _setting_same_article_title(df, title_col, lc_title_col, similar, norm_title):
    print("\n    Setting same publication's title...")
    titles_list = df[title_col].to_list()
    title_df = pd.DataFrame(titles_list, columns=[lc_title_col])
    lines_nb = len(title_df)
    t1_idx = 0
    for t1 in title_df[lc_title_col]:
        t1_idx += 1
        for t2 in title_df[lc_title_col]:
            if t2!=t1 and (len(t1)>bp_sg.LENGTH_THRESHOLD and len(t2)>bp_sg.LENGTH_THRESHOLD):
                t1_set, t2_set = set(t1.split()), set(t2.split())
                common_words = t2_set.intersection(t1_set)
                t1_specific_words, t2_specific_words = (t1_set - common_words), (t2_set - common_words)
                similarity = round(similar(t1, t2)*100)
                if (similarity>bp_sg.SIMILARITY_THRESHOLD) or (t1_specific_words==set() or t2_specific_words==set()):
                    title_df.loc[title_df[lc_title_col]==t2] = t1
            print(f"        Number of titles searched: {t1_idx}  / {lines_nb}", end="\r")
    title_df[lc_title_col] = title_df[lc_title_col].str.lower()
    title_df[lc_title_col] = title_df[lc_title_col].apply(norm_title)
    df.reset_index(inplace=True, drop=True)
    same_title_df = pd.concat([df, title_df], axis=1)
    return same_title_df


def _setting_issn(df, same_journal_col, issn_col):
    dfs_list = []
    for _, journal_dg in df.groupby(same_journal_col):
        if bp_sg.UNKNOWN in journal_dg[issn_col].to_list():
            journal_dg[issn_col] = _find_value_to_keep(journal_dg, issn_col)
        dfs_list.append(journal_dg)
    if dfs_list!=[]:
        issn_df = pd.concat(dfs_list)
    else:
        issn_df = df.copy()
    return issn_df


def _setting_doi(df, lc_title_col, doi_col):
    dfs_list = []
    for _, title_dg in df.groupby(lc_title_col):
        if bp_sg.UNKNOWN in title_dg[doi_col].to_list():
            title_dg[doi_col] = _find_value_to_keep(title_dg, doi_col)
        dfs_list.append(title_dg)
    if dfs_list != []:
        doi_df = pd.concat(dfs_list)
    else:
        doi_df = df.copy()
    return doi_df


def _setting_doc_type(df, doi_col, doc_type_col):
    dfs_list = []
    for _, doi_dg in df.groupby(doi_col):
        if bp_sg.UNKNOWN in doi_dg[doc_type_col].to_list():
            doi_dg[doc_type_col] = _find_value_to_keep(doi_dg, doc_type_col)
        dfs_list.append(doi_dg)
    if dfs_list != []:
        doctype_df = pd.concat(dfs_list)
    else:
        doctype_df = df.copy()
    return doctype_df


def _setting_same_doi(df, cols_list):
    authors_col, lc_doc_type_col, issn_col, page_col, doi_col, lc_title_col, lc_doi_col = cols_list
    dfs_list = []
    for _, sub_df in df.groupby([authors_col, lc_doc_type_col, issn_col, page_col]):
        dois_nb = len(list(set(sub_df[doi_col].to_list())))
        titles_nb = len(list(set(sub_df[lc_title_col].to_list())))
        if bp_sg.UNKNOWN in sub_df[doi_col].to_list() and titles_nb>1:
            sub_df[doi_col] = _find_value_to_keep(sub_df, doi_col)
            sub_df[lc_title_col] = _find_value_to_keep(sub_df, lc_title_col)
        dfs_list.append(sub_df)
    if dfs_list != []:
        title_same_doi_df = pd.concat(dfs_list)
    else:
        title_same_doi_df = df.copy()
    title_same_doi_df[lc_doi_col] = title_same_doi_df[doi_col].str.lower()
    return title_same_doi_df


def _setting_same_first_author_name(df, cols_list):
    (lc_doc_type_col, issn_col, lc_title_col, page_col,
     pub_id_col, authors_col, lc_doi_col) = cols_list
    dfs_list = []
    for _, sub_df in df.groupby([lc_doc_type_col, issn_col, lc_title_col, page_col]):
        authors_list = list(set(sub_df[authors_col].to_list()))
        dois_list = list(set(sub_df[lc_doi_col].to_list()))
        authors_nb = len(authors_list)
        dois_nb = len(dois_list)
        if authors_nb>1 and bp_sg.UNKNOWN in dois_list:
            sub_df[authors_col] = _find_value_to_keep(sub_df, authors_col, length_max=True)
            sub_df[lc_doi_col] = _find_value_to_keep(sub_df, lc_doi_col)
        dfs_list.append(sub_df)
    if dfs_list!=[]:
        same_author_df = pd.concat(dfs_list)
    else:
        same_author_df = df.copy()
    same_author_df.sort_values(by=[pub_id_col], inplace=True)
    return same_author_df


def _dropping_duplicate_article1(df, cols_list):
    lc_doi_col, title_col, doc_type_col, lc_title_col, lc_doc_type_col = cols_list
    dfs_list = []
    for doi, dg in df.groupby(lc_doi_col):
        if doi!=bp_sg.UNKNOWN:
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


def _dropping_duplicate_article2(df, cols_list):
    lc_title_col, lc_doc_type_col, same_journal_col, lc_doi_col, pub_id_col = cols_list
    dfs_list = []
    for idx, dg in df.groupby([lc_title_col, lc_doc_type_col, same_journal_col]):
        if len(dg) < 3:
            # Deduplicating article lines with same title, document type, first author and journal
            # and also with same DOI if not bp_sg.UNKNOWN
            dg[lc_doi_col] = _find_value_to_keep(dg, lc_doi_col)
            dg.drop_duplicates(subset = [lc_doi_col], keep='first', inplace=True)
        else:
            # Dropping Publications data with DOI bp_sg.UNKNOWN from group of publications with same title,
            # document type, first author and journal but different DOIs
            unkown_indices = dg[dg[lc_doi_col]==bp_sg.UNKNOWN].index
            dg.drop(unkown_indices,inplace = True)
            pub_ids_list = [x for x in dg[pub_id_col]]
            warning = (f'WARNING: Multiple DOI values for same title, document type, first author and journal '
                                  f'are found in the group of publication data with IDs {pub_ids_list} '
                                  f'in "_deduplicate_articles" function '
                                  f'called by "parsing_concatenate_deduplicate" function '
                                  f'of "BiblioParsingConcat.py" module.\n'
                                  f'Publications data with DOIs "{bp_sg.UNKNOWN}" has been droped.')
            print(warning)
        dfs_list.append(dg)
    if dfs_list!=[]:
        dedup_df = pd.concat(dfs_list)
    else:
        dedup_df = df.copy()
    dedup_df = dedup_df.drop([lc_title_col, lc_doc_type_col, lc_doi_col], axis=1)
    dedup_df.sort_values(by=[pub_id_col], inplace=True)
    return dedup_df


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
    print("\nDeduplicating publications main data...")

    # Setting useful internal functions

    def _norm_doctype(doctype):
        lc_doctype = doctype.lower()
        norm_doctype = lc_doctype
        for key, values in lc_doctype_dic.items():
            if lc_doctype in values:
                norm_doctype = key
            else:
                pass
        return norm_doctype

    def _norm_title(title):
        for init_symb, new_symb in bp_gg.TITLE_SYMB_CHANGE_DIC.items():
            title.replace(init_symb, new_symb)
        new_title = title.strip()
        return new_title

    similar = lambda a, b: SequenceMatcher(None, a, b).ratio()
    norm_title = lambda x: _norm_title(x)

    # Setting useful column names
    cols_keys = ['pub_id_col', 'authors_col', 'page_col', 'doi_col',
                 'doc_type_col', 'title_col', 'issn_col',
                 'lc_title_col', 'lc_doc_type_col', 'lc_doi_col',
                 'norm_journal_col', 'same_journal_col']
    (pub_id_col, authors_col, page_col, doi_col,
     doc_type_col, title_col, issn_col,
     lc_title_col, lc_doc_type_col, lc_doi_col,
     norm_journal_col, same_journal_col) = [cols_dic[key] for key in cols_keys]

    # Setting lower case doc-type dict for normalization of doc-types
    lc_doctype_dic = {}
    for key, values in bp_sg.DIC_DOCTYPE.items():
        lc_doctype_dic[key.lower()] = [x.lower() for x in values]

    # Setting same journal name for similar journal names
    inter1_articles_concat_df = _setting_same_journal_name(init_articles_concat_df, norm_journal_col,
                                                           same_journal_col, similar)
    print("\n    Column with unique journal name added to the initial concatenated-data of the publications.")

    # Setting same article title for similar article title
    inter2_articles_concat_df = _setting_same_article_title(inter1_articles_concat_df, title_col,
                                                            lc_title_col, similar, norm_title)
    print("\n    Publications' titles standardized.")

    # Setting issn when unknown for given article ID using available issn values
    # of journals of same normalized names from other article IDs
    issn_articles_concat_df = _setting_issn(inter2_articles_concat_df, same_journal_col, issn_col)
    print("\n    Available ISSN value set common to journals with same name.")

    # Adding useful temporal columns
    issn_articles_concat_df[lc_title_col] = issn_articles_concat_df[lc_title_col].str.lower()
    issn_articles_concat_df[lc_doc_type_col] = issn_articles_concat_df[doc_type_col].apply(lambda x: _norm_doctype(x))
    issn_articles_concat_df[title_col] = issn_articles_concat_df[title_col].str.strip()

    # Setting DOI when unknown for given article ID using available DOI values
    # of articles of same title from other article IDs
    # Modification on 09-2023
    doi_articles_concat_df = _setting_doi(issn_articles_concat_df, lc_title_col, doi_col)
    print("    Available DOI value set common to publications with same title.")

    # Setting document type when unknown for given article ID using available document type values
    # of articles of same DOI from other article IDs
    # Modification on 09-2023
    doctype_articles_concat_df = _setting_doc_type(doi_articles_concat_df, doi_col, doc_type_col)
    print("    Available document-type value set common to publications with same DOI.")

    # Setting same DOI for similar titles when any DOI is unknown
    # for same first author, page, document type and ISSN
    # Modification on 09-2023
    cols_list = [authors_col, lc_doc_type_col, issn_col, page_col, doi_col, lc_title_col, lc_doi_col]
    title_articles_concat_df = _setting_same_doi(doctype_articles_concat_df, cols_list)
    print("    Available DOI value set common to publications with same first author, page, document type and ISSN.")

    # Setting same first author name for same page, document type and ISSN 
    # when DOI is unknown or DOIs are different
    # Modification on 09-2023
    cols_list = [lc_doc_type_col, issn_col, lc_title_col, page_col,
                 pub_id_col, authors_col, lc_doi_col]
    author_articles_concat_df = _setting_same_first_author_name(title_articles_concat_df, cols_list)
    print("    Same first author name set common to publications with same page, document type and ISSN.")

    # Keeping copy of author_articles_concat_df with completed same_journal_col, issn_col, doi_col and doc_type_col columns
    full_articles_concat_df = author_articles_concat_df.copy()

    # Dropping duplicated publication data after merging by doi or, for unknown doi, by title and document type
    cols_list = [lc_doi_col, title_col, doc_type_col, lc_title_col, lc_doc_type_col]
    doi_articles_dedup_df = _dropping_duplicate_article1(author_articles_concat_df, cols_list)
    print("    Publication data with same DOI deduplicated on DOI except for unknown DOI.")
    print("    Publication data with unknown DOI deduplicated on title and document type.")

    # Dropping duplicated publication data after merging by title, document type and journal
    cols_list = [lc_title_col, lc_doc_type_col, same_journal_col, lc_doi_col, pub_id_col]
    articles_dedup_df = _dropping_duplicate_article2(doi_articles_dedup_df, cols_list)
    print("    Publication data deduplicated on title, document type and journal.")

    # Identifying the set of articles IDs to drop in the other parsing files of the concatenated corpus
    pub_id_set_init = set(full_articles_concat_df[pub_id_col].to_list())
    pub_id_set_end  = set(articles_dedup_df[pub_id_col].to_list())
    pub_id_to_drop  = pub_id_set_init - pub_id_set_end
    print("    List of publication identifiers to drop in other concatenated parsing data built.")

    # Setting usefull prints
    articles_nb_init = len(full_articles_concat_df)
    articles_nb_end  = len(articles_dedup_df)
    articles_nb_drop = articles_nb_init - articles_nb_end

    if verbose:
        print('\nDeduplication results:')
        print(f'    Initial publications number: {articles_nb_init}')
        print(f'    Final publications number: {articles_nb_end}')
        warning = (f'    WARNING: {articles_nb_drop} publications have been dropped as duplicates')
        print(warning)

    return (articles_dedup_df, pub_id_to_drop)


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
    filt = (item_df[pub_id_col].isin(pub_ids_to_drop))
    item_dg = item_df[~filt].copy()
    item_dg.sort_values(by=[pub_id_col], inplace=True)

    if second_col:
        item_dg.sort_values(by=[pub_id_col, second_col], inplace=True)
    return item_dg


def deduplicate_parsing(concat_parsing_dict, norm_inst_status=False, inst_types_file_path=None,
                        country_affiliations_file_path=None, country_towns_file=None,
                        country_towns_folder_path=None, verbose=False):
    """Deduplicates parsing data from the concatenated parsing data.

    It proceeds with deduplication of publications data using the `_deduplicate_articles` internal 
    function of the module. 
    Then, it rationalizes the content of the other parsing data using the publication identifiers
    of the droped publications data using the `_deduplicate_item_df` internal function of the module.
    The outputs are the deduplicated parsing data of the corpus.

    Args:
        concat_parsing_dict (dict): Dict with keys as items parsing (str) and values (dataframe) as \
        the data resulting from the concatenation of corpuses parsings.
        norm_inst_status (bool): If true (dafault= False)of normalized institutions and of not-yet \
        normalized institutions.
        inst_types_file_path (path): The full path to the data of institutions-types used to normalize \
        the affiliations, optional (default=None).
        country_affiliations_file_path (path): The full path to the data per country of raw affiliations \
        per normalized one, optional (default=None).
        country_towns_file (str): The name of the file of the data of towns per country, optional (default=None).
        country_towns_folder_path (path): The full path to the folder where the 'country_towns_file' file \
        is available, optional (default=None).
        verbose (bool): True for allowing control prints (default: False).
    Returns:
        (dict): Dict with keys as parsing items (str) and values (dataframe) as the deduplicated data.
    """
    # Setting useful col names
    cols_dic = _set_dedup_cols()
    cols_keys = ['pub_id_col', 'author_idx_col', 'address_idx_col', 'country_addr_idx_col',
                 'inst_addr_idx_col', 'auth_inst_auth_idx_col']
    (pub_id_col, author_idx_col, address_idx_col, country_addr_idx_col,
     inst_addr_idx_col, auth_inst_auth_idx_col) = [cols_dic[key] for key in cols_keys]
    cols_list = [cols_dic[key] for key in cols_keys]
    pub_id_col = cols_list[0]

    # Setting second cols for sorting item's data after deduplication for selected items
    second_col_items_list = [bp_sg.PARSING_ITEMS_LIST[x] for x in range(1, 6)]
    sorting_second_col_dict = dict(zip(second_col_items_list, cols_list[1:]))

    # Setting useful items' lists and items' values for deduplication process
    full_items_list = list(concat_parsing_dict.keys())
    sub_items_list = [bp_sg.PARSING_ITEMS_LIST[x] for x in [0, 2, 12, 13]]
    (articles_item, addresses_item, norm_inst_item, raw_inst_item) = sub_items_list
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
        if item in sorting_second_col_dict.keys():
            second_col = sorting_second_col_dict[item]
        dedup_parsing_dict[item] = _deduplicate_item_df(pub_ids_to_drop, item_df, pub_id_col, second_col)

    if norm_inst_status:
        # Creating data of normalized institutions and of not-yet normalized institutions
        address_df = dedup_parsing_dict[addresses_item]
        return_tup = build_norm_raw_institutions(address_df,
                                                 inst_types_file_path=inst_types_file_path,
                                                 country_affiliations_file_path=country_affiliations_file_path,
                                                 country_towns_file=country_towns_file,
                                                 country_towns_folder_path=country_towns_folder_path,
                                                 verbose=False)
        _, norm_institution_df, raw_institution_df, wrong_affil_types_dict = return_tup
        dedup_parsing_dict[norm_inst_item] = norm_institution_df
        dedup_parsing_dict[raw_inst_item] = raw_institution_df

        if wrong_affil_types_dict:
            print("\nWARNING: Uncorrect normalized-affiliation types found in the file: "
                  f"\n         {user_country_affiliations_file_path}"
                  "\n\n         Please, correct the following affiliation types:")
            dict_print(country_affiliations_file_path)

    return dedup_parsing_dict
