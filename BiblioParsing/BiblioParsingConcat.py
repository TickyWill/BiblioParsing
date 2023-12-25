__all__ = ['parsing_concatenate_deduplicate']
    

def _concatenate_item_dfs(item, first_corpus_df, second_corpus_df):
    
    '''The `_concatenate_dat` function concatenates the .dat files having the same name "filename" 
    in the parsing folders of two corpuses referenced as first corpus and second corpus.
     
    Args : 
        filename (string): name of the files to be concatenated.
        path_first_corpus (path): path of the folder where the .dat file "filename" is saved 
                                  for the first corpus.
        path_second_corpus (path): path of the folder where the .dat file "filename" is saved 
                                   for the second corpus.
        path_concat_result (path): path of the folder where the concatenated .dat file will be saved 
                            with the name "filename".
                            
    Returns: 
        (datafframe):.
        
    Note:
        The global 'COL_NAMES' are imported from 'BiblioSpecificGlobals' module of 'BiblioParsing' package.
    
    '''
    # Standard libraries import
    from pathlib import Path

    # 3rd party library imports
    import pandas as pd
    
    # Local imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    
    pub_id_alias = COL_NAMES['pub_id']
    
    # Incrementing the "pub_id_alias" column values of second corpus by first corpus length 
    first_corpus_articles_nb = max(first_corpus_df[pub_id_alias]) + 1
    new_second_corpus_df     = second_corpus_df.copy()    
    new_second_corpus_df[pub_id_alias] = new_second_corpus_df[pub_id_alias] + first_corpus_articles_nb
    
    # Concatenating the two dataframes
    dfs_list  = [first_corpus_df, new_second_corpus_df]
    concat_df = pd.concat(dfs_list)
    concat_df.sort_values([pub_id_alias], inplace = True)
    
    return concat_df
    

def _deduplicate_articles(concat_parsing_dict, verbose = False):
    
    '''The `_deduplicate_articles` uses the concatenated articles list and applies a succesion of filters
    to get rid of duplicated information.
    
    Args :
        path_in (string) : folder path where the .dat file of the concatenated articles list is available.
        
    Returns :
        (list): the list contains a dataframe of articles with no duplicates but unfull information, 
                a list of dataframes each of them containing a line that is a duplicate in the articles dataframe,
                and a list of the duplicate indices.
        
    Notes:
       The globals `BOLD_TEXT` and `LIGHT_TEXT` are imported from 'BiblioGeneralGlobals' module of 'BiblioParsing' package. 
       The globals `COL_NAMES`, `DIC_OUTDIR_PARSING`, `LENGTH_THRESHOLD`, `SIMILARITY_THRESHOLD` and `UNKNOWN` 
       are imported from 'BiblioSpecificGlobals' module of 'BiblioParsing' package.
    
    '''
    # Standard library imports
    from colorama import Fore
    from pathlib import Path
    from difflib import SequenceMatcher
    
    # 3rd party imports
    import pandas as pd
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import BOLD_TEXT
    from BiblioParsing.BiblioGeneralGlobals import LIGHT_TEXT    
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import DIC_DOCTYPE
    from BiblioParsing.BiblioSpecificGlobals import LENGTH_THRESHOLD    
    from BiblioParsing.BiblioSpecificGlobals import NORM_JOURNAL_COLUMN_LABEL    
    from BiblioParsing.BiblioSpecificGlobals import PARSING_ITEMS
    from BiblioParsing.BiblioSpecificGlobals import SIMILARITY_THRESHOLD
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    # Internal functions
    def _setting_same_journal_name(df_articles_concat_init):
        journals_list = df_articles_concat_init[journal_alias].to_list()
        df_journal = pd.DataFrame(journals_list, columns = [norm_journal_alias])
        for j1 in df_journal[norm_journal_alias]:     
            for j2 in df_journal[norm_journal_alias]:
                if j2 != j1 and (len(j1) > LENGTH_THRESHOLD and len(j2) > LENGTH_THRESHOLD):
                    j1_set, j2_set = set(j1.split()), set(j2.split())
                    common_words =  j2_set.intersection(j1_set)
                    j1_specific_words, j2_specific_words = (j1_set - common_words), (j2_set - common_words)
                    similarity = round(similar(j1,j2)*100)    
                    if (similarity > SIMILARITY_THRESHOLD) or (j1_specific_words == set() or j2_specific_words == set()):
                        df_journal.loc[df_journal[norm_journal_alias] == j2] = j1
        df_articles_concat_init.reset_index(inplace=True, drop=True)
        df_articles_concat_inter1 = pd.concat([df_articles_concat_init, df_journal], axis = 1)
        return df_articles_concat_inter1
    
    def _setting_same_article_title(df_articles_concat_inter1):
        titles_list = df_articles_concat_inter1[title_alias].to_list()
        df_title = pd.DataFrame(titles_list, columns = [lc_title_alias])
        for t1 in df_title[lc_title_alias]:     
            for t2 in  df_title[lc_title_alias]:
                if t2 != t1 and (len(t1) > LENGTH_THRESHOLD and len(t2) > LENGTH_THRESHOLD):
                    t1_set, t2_set = set(t1.split()), set(t2.split())
                    common_words =  t2_set.intersection(t1_set)
                    t1_specific_words, t2_specific_words = (t1_set - common_words), (t2_set - common_words)
                    similarity = round(similar(t1,t2)*100)    
                    if (similarity > SIMILARITY_THRESHOLD) or (t1_specific_words == set() or t2_specific_words == set()):
                        df_title.loc[df_title[lc_title_alias] == t2] = t1
        df_title[lc_title_alias] = df_title[lc_title_alias].str.lower()
        df_title[lc_title_alias] = df_title[lc_title_alias].apply(norm_title)
        df_articles_concat_inter1.reset_index(inplace=True, drop=True)
        df_articles_concat_inter2 = pd.concat([df_articles_concat_inter1, df_title], axis = 1) 
        return df_articles_concat_inter2
    
    def _setting_issn(df_articles_concat_inter2):
        df_list = []
        for _, journal_dg in df_articles_concat_inter2.groupby(norm_journal_alias):
            if UNKNOWN in journal_dg[issn_alias].to_list(): # Modification on 08-2023
                journal_dg[issn_alias] = _find_value_to_keep(journal_dg, issn_alias)             
            df_list.append(journal_dg)
        if df_list != []:
            df_articles_concat_issn = pd.concat(df_list)
        else:
            df_articles_concat_issn = df_articles_concat_inter.copy()
        return df_articles_concat_issn
    
    def _setting_doi(df_articles_concat_issn):
        df_list = []
        for _, title_dg in df_articles_concat_issn.groupby(lc_title_alias):
            if UNKNOWN in title_dg[doi_alias].to_list():
                title_dg[doi_alias] = _find_value_to_keep(title_dg,doi_alias)
            df_list.append(title_dg) 
        if df_list != []:
            df_articles_concat_doi = pd.concat(df_list)
        else:
            df_articles_concat_doi = df_articles_concat_issn.copy() 
        return df_articles_concat_doi
    
    def _setting_doc_type(df_articles_concat_doi):
        df_list = []
        for _, doi_dg in df_articles_concat_doi.groupby(doi_alias):
            if UNKNOWN in doi_dg[doc_type_alias].to_list(): 
                doi_dg[doc_type_alias] = _find_value_to_keep(doi_dg,doc_type_alias)   
            df_list.append(doi_dg) 
        if df_list != []:
            df_articles_concat_doctype = pd.concat(df_list)
        else:
            df_articles_concat_doctype = df_articles_concat_doi.copy()
        return df_articles_concat_doctype
    
    def _setting_same_doi(df_articles_concat_doctype):
        df_list = []   
        for _, sub_df in df_articles_concat_doctype.groupby([author_alias,lc_doc_type_alias,issn_alias,page_alias]):                      
            dois_nb = len(list(set(sub_df[doi_alias].to_list())))
            titles_nb = len(list(set(sub_df[lc_title_alias].to_list())))
            if UNKNOWN in sub_df[doi_alias].to_list() and titles_nb>1:                       
                sub_df[doi_alias]      = _find_value_to_keep(sub_df,doi_alias)
                sub_df[lc_title_alias] = _find_value_to_keep(sub_df,lc_title_alias)
            df_list.append(sub_df) 
        if df_list != []:
            df_articles_concat_title = pd.concat(df_list)
        else:
            df_articles_concat_title = df_articles_concat_doctype.copy()
        return df_articles_concat_title
    
    def _setting_same_first_author_name(df_articles_concat_title):
        df_list = []   
        for _, sub_df in df_articles_concat_title.groupby([lc_doc_type_alias, issn_alias, lc_title_alias, page_alias]):        
            pub_ids      = list(set(sub_df[pub_id_alias].to_list()))
            authors_list = list(set(sub_df[author_alias].to_list()))
            authors_nb   = len(authors_list)
            dois_list    = list(set(sub_df[doi_alias].to_list()))
            dois_nb      = len(dois_list)       
            if authors_nb >1 and UNKNOWN in dois_list :                        
                sub_df[author_alias] = _find_value_to_keep(sub_df,author_alias)
                sub_df[doi_alias]    = _find_value_to_keep(sub_df,doi_alias)
            df_list.append(sub_df) 
        if df_list != []:
            df_articles_concat_author = pd.concat(df_list)
        else:
            df_articles_concat_author = df_articles_concat_title.copy()
        df_articles_concat_author.sort_values(by=[pub_id_alias], inplace = True)
        return df_articles_concat_author
    
    def _dropping_duplicate_article1(df_articles_concat_author):
        df_list = []
        for doi, dg in  df_articles_concat_author.groupby(doi_alias):
            if doi != UNKNOWN:
                # Deduplicating article lines by DOI
                dg[title_alias]    = _find_value_to_keep(dg,title_alias)
                dg[doc_type_alias] = _find_value_to_keep(dg,doc_type_alias)
                dg.drop_duplicates(subset = [doi_alias], keep = 'first', inplace = True)

            else:
                # Deduplicating article lines without DOI by title and document type
                dg.drop_duplicates(subset = [lc_title_alias,lc_doc_type_alias], keep = 'first', inplace = True)
            df_list.append(dg)
        df_articles_concat = pd.concat(df_list)
        return df_articles_concat
    
    def _dropping_duplicate_article2(df_articles_concat):
        df_list = []   
        for idx, dg in df_articles_concat.groupby([lc_title_alias,lc_doc_type_alias,norm_journal_alias]): 
            if len(dg) < 3:
                # Deduplicating article lines with same title, document type, first author and journal
                # and also with same DOI if not UNKNOWN
                dg[doi_alias] = _find_value_to_keep(dg,doi_alias)
                dg.drop_duplicates(subset = [doi_alias],keep = 'first',inplace=True)          
            else:   
                # Dropping article lines with DOI UNKNOWN from group of articles with same title, 
                # document type, first author and journal but different DOIs 
                unkown_indices = dg[dg[doi_alias] == UNKNOWN].index
                dg.drop(unkown_indices,inplace = True)
                pub_id_list = [x for x in dg[pub_id_alias]]
                warning = (f'WARNING: Multiple DOI values for same title, document type, first author and journal '
                                      f'are found in the group of article lines with IDs {pub_id_list} '
                                      f'in "_deduplicate_articles" function '
                                      f'called by "parsing_concatenate_deduplicate" function '
                                      f'of "BiblioParsingConcat.py" module.\n'
                                      f'Article lines with DOIs "{UNKNOWN}" has been droped.')                      
                print(Fore.BLUE + warning + Fore.WHITE)
            df_list.append(dg) 
        if df_list != []:
            df_articles_dedup = pd.concat(df_list)
        else:
            df_articles_dedup = df_articles_concat
        df_articles_dedup = df_articles_dedup.drop([lc_title_alias, lc_doc_type_alias], axis = 1)
        df_articles_dedup.sort_values(by = [pub_id_alias], inplace = True)
        return df_articles_dedup

    def _norm_doctype(lc_doctype):
        norm_doctype = lc_doctype
        for key, values in lc_dic_doctype.items():            
            if lc_doctype in values:
                norm_doctype = key
            else:
                pass
        return norm_doctype
        
    def _find_value_to_keep(dg, column_name):
        col_values_list = dg[column_name].to_list()
        col_values_list = list(dict.fromkeys(col_values_list)) 
        if UNKNOWN in col_values_list: col_values_list.remove(UNKNOWN) 
        value_to_keep = col_values_list[0] if len(col_values_list)>0 else UNKNOWN
        return value_to_keep 
    
    similar = lambda a,b:SequenceMatcher(None, a, b).ratio()
    
    norm_title = lambda x: x.replace(" - ", "-").replace("(","").replace(")","").replace(" :", ": ").replace("-", " ").replace("  ", " ").strip()    
    
    # Setting lower case doc-type dict for normalization of doc-types
    lc_dic_doctype = {}
    for key, values in DIC_DOCTYPE.items(): lc_dic_doctype[key.lower()] = [x.lower() for x in values]
    
    # Defining aliases for text format control
    bold_text = BOLD_TEXT
    light_text = LIGHT_TEXT
    
    # Defining aliases for column names of the articles file (.dat)
    pub_id_alias   = COL_NAMES['pub_id']
    author_alias   = COL_NAMES['articles'][1]
    page_alias     = COL_NAMES['articles'][5]
    doi_alias      = COL_NAMES['articles'][6]
    doc_type_alias = COL_NAMES['articles'][7]
    title_alias    = COL_NAMES['articles'][9]
    issn_alias     = COL_NAMES['articles'][10]
    journal_alias  = NORM_JOURNAL_COLUMN_LABEL
    articles_item_alias = PARSING_ITEMS["articles"]
    
    # Setting the name of a temporal column of titles in lower case 
    # to be added to working dataframes for case unsensitive dropping of duplicates
    lc_title_alias    = COL_NAMES['temp_col'][0] 
    lc_doc_type_alias = COL_NAMES['temp_col'][5] 
    
    # Setting the name of a temporal column of journals normalized 
    # to be added to working dataframes for dropping of duplicates
    norm_journal_alias = COL_NAMES['temp_col'][1]
    
    # Setting initial articles df
    df_articles_concat_init = concat_parsing_dict[articles_item_alias]

    # Setting same journal name for similar journal names    
    df_articles_concat_inter1 = _setting_same_journal_name(df_articles_concat_init)
    
    # Setting same article title for similar article title
    df_articles_concat_inter2 = _setting_same_article_title(df_articles_concat_inter1)
    
    # Setting issn when unknown for given article ID using available issn values 
    # of journals of same normalized names from other article IDs
    df_articles_concat_issn = _setting_issn(df_articles_concat_inter2)
    
    # Adding useful temporal columns
    df_articles_concat_issn[lc_title_alias]    = df_articles_concat_issn[lc_title_alias].str.lower()
    df_articles_concat_issn[lc_doc_type_alias] = df_articles_concat_issn[doc_type_alias].apply(lambda x: _norm_doctype(x.lower()))
    df_articles_concat_issn[title_alias]       = df_articles_concat_issn[title_alias].str.strip()
    
    # Setting DOI when unknown for given article ID using available DOI values 
    # of articles of same title from other article IDs
    # Modification on 09-2023
    df_articles_concat_doi = _setting_doi(df_articles_concat_issn)

    # Setting document type when unknown for given article ID using available document type values 
    # of articles of same DOI from other article IDs
    # Modification on 09-2023
    df_articles_concat_doctype = _setting_doc_type(df_articles_concat_doi)    
   
    # Setting same DOI for similar titles when any DOI is unknown
    # for same first author, page, document type and ISSN
    # Modification on 09-2023
    df_articles_concat_title = _setting_same_doi(df_articles_concat_doctype)
    
    # Setting same first author name for same page, document type and ISSN 
    # when DOI is unknown or DOIs are different
    # Modification on 09-2023
    df_articles_concat_author = _setting_same_first_author_name(df_articles_concat_title)
    
    # Keeping copy of df_articles_concat with completed norm_journal_alias, issn_alias, doi_alias and doc_type_alias columns
    df_articles_concat_full = df_articles_concat_author.copy()
        
    # Dropping duplicated article lines after merging by doi or, for unknown doi, by title and document type 
    df_articles_concat = _dropping_duplicate_article1(df_articles_concat_author)
    
    # Dropping duplicated article lines after merging by titles, document type and journal
    df_articles_dedup = _dropping_duplicate_article2(df_articles_concat)
    
    # Identifying the set of articles IDs to drop in the other parsing files of the concatenated corpus
    pub_id_set_init = set(df_articles_concat_full[pub_id_alias].to_list())
    pub_id_set_end  = set(df_articles_dedup[pub_id_alias].to_list())    
    pub_id_to_drop  = pub_id_set_init - pub_id_set_end 
    
    # Setting usefull prints
    articles_nb_init = len(df_articles_concat_full)    
    articles_nb_end  = len(df_articles_dedup)
    articles_nb_drop = articles_nb_init - articles_nb_end
    
    if verbose:
        print('\nDeduplication results:')
        print(f'    Initial articles number: {articles_nb_init}')
        print(f'    Final articles number: {articles_nb_end}')
        warning = (f'    WARNING: {articles_nb_drop} articles have been dropped as duplicates')
        print(Fore.BLUE +  bold_text + warning + light_text + Fore.WHITE)
                                
    return (df_articles_dedup, pub_id_to_drop)


def _deduplicate_item_df(pub_id_to_drop, item, item_df):    
    '''The `_deduplicate_item_df` function drops the lines corresponding to `pub_id_to_drop' list of articles IDs
    in the "item_df" issued from concatenation of parsing dfs of corpuses, exept the one of articles list.
    
    Args : 
       pub_id_to_drop (list): The list of articles IDs which lines should be dropped from "item_df".
       item (str): The item targetted by the deduplication.
       item_df (df): The df targetted by the deduplication.               
        
    Returns :
       (df): Dataframe where the lines corresponding to `pub_id_to_drop' has been dropped.
        
    Notes:
       The globals 'COL_NAMES' and 'PARSING_ITEMS' are imported from 'BiblioSpecificGlobals' module 
       of 'BiblioParsing' package.
       
    '''
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import PARSING_ITEMS
    
    # Setting useful aliases
    pub_id_alias = COL_NAMES['pub_id']
    authors_item_alias      = PARSING_ITEMS["authors"]
    addresses_item_alias    = PARSING_ITEMS["addresses"]
    countries_item_alias    = PARSING_ITEMS["countries"]
    institutions_item_alias = PARSING_ITEMS["institutions"]
    auth_inst_item_alias    = PARSING_ITEMS["authors_institutions"]

    filt = (item_df[pub_id_alias].isin(pub_id_to_drop))
    item_df = item_df[~filt]
    item_df.sort_values([pub_id_alias], inplace = True)
    
    second_col_sorting_dict = {authors_item_alias      : COL_NAMES['authors'][1],
                               addresses_item_alias    : COL_NAMES['address'][1],
                               countries_item_alias    : COL_NAMES['country'][1],
                               institutions_item_alias : COL_NAMES['institution'][1],
                               auth_inst_item_alias    : COL_NAMES['auth_inst'][1],
                              }
    
    if item in second_col_sorting_dict.keys():
        item_df.sort_values([pub_id_alias,second_col_sorting_dict[item]], inplace = True)
        
    return item_df


def _concatenate_parsing(first_parsing_dict, second_parsing_dict, inst_filter_list = None):
    ''' The `_concatenate_parsing` function concatenates parsing dfs of two corpuses 
    using the `_concatenate_item_dfs` internal functions. 
    Then it proceeds with extending the "author with institutions" parsing df 
    using the `extend_author_institutions function.
    The outputs are the parsing dfs of the concatenated corpus.
    
    Args:
        first_parsing_dict (dict): Dict with keys as items parsing and values as the dfs 
                                   resulting from the parsing of the first corpus.
        second_parsing_dict (dict): Dict with keys as items parsing and values as the dfs 
                                    resulting from the parsing of the second corpus.
        inst_filter_list (list): The affiliations filter list of tuples (institution, country)
                                 with default value set to None. 
        
    Returns: 
        (dict): Dict with keys as items parsing and values as the concatenated dfs.
        
    Note:
        The function 'extend_author_institutions' is imported from 'BiblioParsingInstitutions' module 
        of 'BiblioParsing' package.
                                  
    '''

    # Local library imports
    from BiblioParsing.BiblioParsingInstitutions import extend_author_institutions
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import PARSING_ITEMS
    
    # Setting useful aliases
    sec_inst_item_alias = PARSING_ITEMS["authors_institutions"]
    
    # Getting a list of the common items of the parsing dicts
    first_items_set   = set(first_parsing_dict.keys())
    second_items_set  = set(second_parsing_dict.keys())
    common_items_list = list(first_items_set.intersection(second_items_set))    
       
    # Concatenating the dicts of wos and scopus corpuses,item by item of the common_items_list
    concat_parsing_dict = {}
    for item in common_items_list: 
        concat_parsing_dict[item] = _concatenate_item_dfs(item, first_parsing_dict[item], second_parsing_dict[item])

    # Extending the author with institutions parsing df
    if inst_filter_list: 
        concat_parsing_dict[sec_inst_item_alias] = extend_author_institutions(sec_inst_item_alias,
                                                                              concat_parsing_dict[sec_inst_item_alias],
                                                                              inst_filter_list)    
    return concat_parsing_dict


def _deduplicate_parsing(concat_parsing_dict):
    ''' The `_deduplicate_parsing` function deduplicate parsing dfs of two corpuses. 
    It proceeds with deduplication of article lines using the `_deduplicate_articles` internal function.
    Then, it rationalizes the content of the other parsing dfs using the IDs of the droped articles lines
    in the `_deduplicate_item_df` internal function.
    The outputs are the parsing dfs of the deduplicated corpus.
    
    Args:
        concat_parsing_dict (dict): Dict with keys as items parsing and values as the dfs 
                                    resulting from the concatenation of corpuses parsings.        
    Returns: 
        (dict): Dict with keys as items parsing and values as the deduplicated dfs.
                                  
    '''
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import PARSING_ITEMS
    
    # Setting useful aliases
    articles_item_alias = PARSING_ITEMS["articles"]
    
    # Getting a list of the items of the parsing dict to deduplicate
    items_list = list(concat_parsing_dict.keys())    
       
    # Getting rid of duplicates 
    df_articles_dedup, pub_id_to_drop = _deduplicate_articles(concat_parsing_dict)

    dedup_parsing_dict = {}
    dedup_parsing_dict[articles_item_alias] = df_articles_dedup
    items_list_wo_articles = items_list
    items_list_wo_articles.remove(articles_item_alias)
    for item in items_list_wo_articles:
        dedup_parsing_dict[item] = _deduplicate_item_df(pub_id_to_drop, item, concat_parsing_dict[item])

    return dedup_parsing_dict


def parsing_concatenate_deduplicate(first_parsing_dict, second_parsing_dict, inst_filter_list = None):
    ''' The `parsing_concatenate_deduplicate` function concatenates the parsing dataframes of two corpuses 
    using the `_concatenate_parsing` local function. Then it proceeds with deduplication of the concatenated dataframes 
    using the `_deduplicate_parsing` local function.
    
    Args:
        first_parsing_dict (dict): Dict with keys as items parsing and values as the dfs 
                                   resulting from the parsing of the first corpus.
        second_parsing_dict (dict): Dict with keys as items parsing and values as the dfs 
                                    resulting from the parsing of the second corpus.
        inst_filter_list (list): The affiliations filter list of tuples (institution, country)
                                 with default value set to None. 
        
    Returns: 
        (tuple): A tuple of two dicts with keys as items parsing and values, for the first dict 
                 as the concatenated dfs and for the second dict as the deduplicated dfs.
        
    Note:
    
                                  
    '''     
    
    # Concatenating the two parsings
    concat_parsing_dict = _concatenate_parsing(first_parsing_dict, second_parsing_dict,  
                                               inst_filter_list = inst_filter_list)
    print(f'\nParsings successfully concatenated')

    # Deduplicating the concatenation of the two parsings
    dedup_parsing_dict = _deduplicate_parsing(concat_parsing_dict)
    print(f'\nParsings successfully deduplicated')
    
    return (concat_parsing_dict, dedup_parsing_dict)


    
