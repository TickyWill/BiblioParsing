__all__ = ['parsing_concatenate_deduplicate',]


def parsing_concatenate_deduplicate(useful_path_list, inst_filter_list = None):
    ''' The `parsing_concatenate_deduplicate` function concatenates parsing files of two corpuses 
    using the `_get_common_files` and `_concatenate_dat` internal functions. 
    Then it proceeds with deduplication of article lines using the `_deduplicate_articles` internal function.
    Finally, it rationalizes the content of the other parsing files using the IDs of the droped articles lines
    in the `_deduplicate_dat` internal function.
    The outputs are the parsing files of the concatenated corpus and the deduplicated corpus saved in dedicated folders.
    Control files which names are set in the globals 'CONCATENATED_XLSX' and 'DEDUPLICATED_XLSX' are also saved 
    as EXCEL files in the same folders.
    
    Args:
        useful_path_list (list): list of paths of the folders where the corpuses are parsed.
        inst_filter_list (list): the affiliations filter list of tuples (institution, country)
                                 with default value set to None. 
        
    Returns: 
        None.
        
    Note:
        The globals 'COL_NAMES', 'CONCATENATED_XLSX', 'DEDUPLICATED_XLSX' and 'DIC_OUTDIR_PARSING' 
        are imported from 'BiblioSpecificGlobals' module of 'BiblioAnalysis_Utils' package.
        The function 'extend_author_institutions' is imported from 'BiblioParsingInstitutions' module 
        of 'BiblioAnalysis_Utils' package.
                                  
    '''
    
    # Standard libraries import
    import os
    from pathlib import Path

    # 3rd party library imports
    import pandas as pd

    # Local imports
    from BiblioAnalysis_Utils.BiblioParsingInstitutions import extend_author_institutions
    
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COL_NAMES
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import NORM_JOURNAL_COLUMN_LABEL
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import CONCATENATED_XLSX
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DEDUPLICATED_XLSX
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_OUTDIR_PARSING
    
    # Setting global aliases
    articles_dat_alias = DIC_OUTDIR_PARSING['A']
    norm_journal_alias = COL_NAMES['temp_col'][1]

    # Setting the useful paths
    path_scopus_parsing = useful_path_list[0]
    path_wos_parsing = useful_path_list[1]
    path_concat_parsing = useful_path_list[2]
    path_rational_parsing = useful_path_list[3]        

    # Getting a list of the .dat files to be concatenated
    files_list = sorted(_get_common_files(path_scopus_parsing, path_wos_parsing))

    # Concatenating the .dat files having the same same name .dat file of the list of files files_list 
    # of the first corpus with the one of the second corpus 
    for file in files_list: 
        _concatenate_dat(file, path_scopus_parsing, path_wos_parsing, path_concat_parsing)
    print(f'\nParsing files successfully concatenated and saved in:\n{path_concat_parsing}')
    
    # Setting the secondary institutions
    if inst_filter_list: 
        # Extending the author with institutions parsing file
        extend_author_institutions(path_concat_parsing, inst_filter_list)

    # Saving concatenated articles list to .xlsx file for checking concatenation
    df_articles_concat = pd.read_csv(path_concat_parsing / Path(articles_dat_alias), 
                                     sep="\t")
    df_articles_concat.to_excel(path_concat_parsing / Path(CONCATENATED_XLSX)) 
    print(f'\nConcatenated articles list file successfully saved as EXCEL file in: \n{path_concat_parsing}')

    # Getting rid of duplicates 
    df_articles_dedup, pub_id_to_drop = _deduplicate_articles(path_concat_parsing)

    # Saving deduplicated articles list to .xlsx file for checking deduplication
    df_articles_dedup.to_excel(path_rational_parsing / Path(DEDUPLICATED_XLSX))
    print(f'\nDeduplicated articles list file successfully saved as EXCEL file in: \n{path_rational_parsing}')    
      
    # Dropping the temporarily created column of normalized journal names in the deduplicated articles dataframe
    df_articles_dedup = df_articles_dedup.drop([NORM_JOURNAL_COLUMN_LABEL, norm_journal_alias], axis = 1)
    
    # Saving deduplicated articles list to .dat file  
    df_articles_dedup.to_csv(path_rational_parsing / Path(articles_dat_alias),
                             index=False,
                             sep='\t')
    print(f'\nArticles list file successfully deduplicated and saved in: \n{path_rational_parsing}')    

    # Updating and saving all other .dat files except the one of articles list
    files_list_wo_articles_dat = files_list
    files_list_wo_articles_dat.remove(articles_dat_alias)
    for file in files_list_wo_articles_dat: 
        _deduplicate_dat(file, pub_id_to_drop, path_concat_parsing, path_rational_parsing)
    print(f'\nOther parsing files successfully deduplicated and saved in: \n{path_rational_parsing}')

    
def _get_common_files(path_first_corpus,path_second_corpus):
    
    '''The `_common_files_new` builds a list of the names of the files present in the parsing folder 
    of two corpuses referenced as first_corpus and second_corpus. 
    
    Args : 
        path_first_corpus (path) : path of the folder where the files of the first_corpus are saved.
        path_second_corpus (path) : path of the folder where the files of the second_corpus are saved.
        
    Returns :
        (list): The list of common files.
        
    '''
        
    # Standard library imports
    import os
    
    # 3rd party library imports
    import pandas as pd
    
    def _list_dat(path_corpus):    
        list_dat =[file for file in os.listdir(path_corpus) if file.endswith('.dat')]            
        return list_dat
    
    list_dir_first_corpus=set(_list_dat(path_first_corpus))
    list_dir_second_corpus=set(_list_dat(path_second_corpus))

    common_list = list_dir_first_corpus.intersection(list_dir_second_corpus)    
    
    return common_list
    

def _concatenate_dat(filename, path_first_corpus, path_second_corpus, path_concat_result):
    
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
        None.
        
    Note:
        The global 'COL_NAMES' are imported from 'BiblioSpecificGlobals' module of 'BiblioAnalysis_Utils' package.
    
    '''
    # Standard libraries import
    from pathlib import Path

    # 3rd party library imports
    import pandas as pd
    
    # Local imports
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COL_NAMES
    
    pub_id_alias = COL_NAMES['pub_id']
    
    # Setting corpuses dataframes from "filename" file
    df_first_corpus = pd.read_csv(path_first_corpus / Path(filename), sep="\t")
    df_second_corpus = pd.read_csv(path_second_corpus / Path(filename), sep="\t")
    
    # Incrementing the "pub_id_alias" column values of second corpus by first corpus length 
    first_corpus_articles_nb = max(df_first_corpus[pub_id_alias]) + 1
    df_second_corpus[pub_id_alias] = df_second_corpus[pub_id_alias] + first_corpus_articles_nb
    
    # Cancatenating the two dataframes
    dfs_list = [df_second_corpus,df_first_corpus]
    df_concat = pd.concat(dfs_list)
    df_concat.sort_values([pub_id_alias],inplace=True)
    
    # Saving the concatenated dataframe 
    df_concat.to_csv(path_concat_result / Path(filename),
                             index=False,
                             sep='\t')
    

def _deduplicate_articles(path_in):
    
    '''The `_deduplicate_articles` uses the concatenated articles list and applies a succesion of filters
    to get rid of duplicated information.
    
    Args :
        path_in (string) : folder path where the .dat file of the concatenated articles list is available.
        
    Returns :
        (list): the list contains a dataframe of articles with no duplicates but unfull information, 
                a list of dataframes each of them containing a line that is a duplicate in the articles dataframe,
                and a list of the duplicate indices.
        
    Notes:
       The globals `BOLD_TEXT` and `LIGHT_TEXT` are imported from 'BiblioGeneralGlobals' module of 'BiblioAnalysis_Utils' package. 
       The globals `COL_NAMES`, `DIC_OUTDIR_PARSING`, `LENGTH_THRESHOLD`, `SIMILARITY_THRESHOLD` and `UNKNOWN` 
       are imported from 'BiblioSpecificGlobals' module of 'BiblioAnalysis_Utils' package.
    
    '''
    # Standard library imports
    from colorama import Fore
    from pathlib import Path
    from difflib import SequenceMatcher
    
    # 3rd party imports
    import pandas as pd
    
    # Local imports
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import BOLD_TEXT
    from BiblioAnalysis_Utils.BiblioGeneralGlobals import LIGHT_TEXT
    
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COL_NAMES
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import NORM_JOURNAL_COLUMN_LABEL
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_DOCTYPE
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_OUTDIR_PARSING
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import LENGTH_THRESHOLD
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import SIMILARITY_THRESHOLD
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import UNKNOWN
    
    # Internal functions
    
    def norm_doctype(lc_doctype):
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
    
    # Setting the name of a temporal column of titles in lower case 
    # to be added to working dataframes for case unsensitive dropping of duplicates
    lc_title_alias    = COL_NAMES['temp_col'][0] 
    lc_doc_type_alias = COL_NAMES['temp_col'][5] 
    
    # Setting the name of a temporal column of journals normalized 
    # to be added to working dataframes for dropping of duplicates
    norm_journal_alias = COL_NAMES['temp_col'][1]
    
    # Setting alias for the articles file (.dat)
    articles_dat_alias = DIC_OUTDIR_PARSING['A']
    
    # Reading the articles file (.dat)
    df_articles_concat_init = pd.read_csv(path_in / Path(articles_dat_alias), 
                                         sep="\t") 

    # Setting same journal name for similar journal names                                            
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
    df_articles_concat_inter1 = pd.concat([df_articles_concat_init, df_journal], axis = 1)
    
    # Setting same article title for similar article title                                           
    titles_list = df_articles_concat_inter1[title_alias].to_list()
    df_title = pd.DataFrame(titles_list, columns = [lc_title_alias])
    for t1 in df_title[lc_title_alias]:     
        for t2 in  df_title[lc_title_alias]:
            if t2 != t1 and (len(j1) > LENGTH_THRESHOLD and len(t2) > LENGTH_THRESHOLD):
                t1_set, t2_set = set(t1.split()), set(t2.split())
                common_words =  t2_set.intersection(t1_set)
                t1_specific_words, t2_specific_words = (t1_set - common_words), (t2_set - common_words)
                similarity = round(similar(t1,t2)*100)    
                if (similarity > SIMILARITY_THRESHOLD) or (t1_specific_words == set() or t2_specific_words == set()):
                    df_title.loc[df_title[lc_title_alias] == t2] = t1
    df_title[lc_title_alias] = df_title[lc_title_alias].str.lower()
    df_title[lc_title_alias] = df_title[lc_title_alias].apply(norm_title)    
    df_articles_concat_inter2 = pd.concat([df_articles_concat_inter1, df_title], axis = 1)    
    
    # Setting issn when unknown for given article ID using available issn values 
    # of journals of same normalized names from other article IDs
    df_list = []
    for _, journal_dg in df_articles_concat_inter2.groupby(norm_journal_alias):
        if UNKNOWN in journal_dg[issn_alias].to_list(): # Modification on 08-2023
            journal_dg[issn_alias] = _find_value_to_keep(journal_dg, issn_alias)             
        df_list.append(journal_dg)
    if df_list != []:
        df_articles_concat_issn = pd.concat(df_list)
    else:
        df_articles_concat_issn = df_articles_concat_inter.copy()
    
    # Adding useful temporal columns
    df_articles_concat_issn[lc_title_alias]    = df_articles_concat_issn[lc_title_alias].str.lower()
    df_articles_concat_issn[lc_doc_type_alias] = df_articles_concat_issn[doc_type_alias].apply(lambda x: norm_doctype(x.lower()))
    df_articles_concat_issn[title_alias]       = df_articles_concat_issn[title_alias].str.strip()
    
    # Setting DOI when unknown for given article ID using available DOI values 
    # of articles of same title from other article IDs
    # Modification on 09-2023 
    df_list = []
    for _, title_dg in df_articles_concat_issn.groupby(lc_title_alias):
        if UNKNOWN in title_dg[doi_alias].to_list():
            title_dg[doi_alias] = _find_value_to_keep(title_dg,doi_alias)
        df_list.append(title_dg) 
    if df_list != []:
        df_articles_concat_doi = pd.concat(df_list)
    else:
        df_articles_concat_doi = df_articles_concat_issn.copy() 

    # Setting document type when unknown for given article ID using available document type values 
    # of articles of same DOI from other article IDs
    # Modification on 09-2023 
    df_list = []
    for _, doi_dg in df_articles_concat_doi.groupby(doi_alias):
        if UNKNOWN in doi_dg[doc_type_alias].to_list(): 
            doi_dg[doc_type_alias] = _find_value_to_keep(doi_dg,doc_type_alias)   
        df_list.append(doi_dg) 
    if df_list != []:
        df_articles_concat_doctype = pd.concat(df_list)
    else:
        df_articles_concat_doctype = df_articles_concat_doi.copy() 
    
    # Setting same DOI for similar titles when any DOI is unknown
    # for same first author, page, document type and ISSN
    # Modification on 09-2023    
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
    
    # Setting same first author name for same page, document type and ISSN 
    # when DOI is unknown or DOIs are different
    # Modification on 09-2023
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
    
    # Keeping copy of df_articles_concat with completed norm_journal_alias, issn_alias, doi_alias and doc_type_alias columns
    df_articles_concat_full = df_articles_concat_author.copy()
        
    # Dropping duplicated article lines after merging by doi or, for unknown doi, by title and document type 
    df_list = []
    for doi, dg in  df_articles_concat_author.groupby(doi_alias):
        if doi != UNKNOWN:
            # Deduplicating article lines by DOI
            dg[title_alias] = _find_value_to_keep(dg,title_alias)
            dg[doc_type_alias] = _find_value_to_keep(dg,doc_type_alias)
            dg.drop_duplicates(subset=[doi_alias], keep='first', inplace=True)
            
        else:
            # Deduplicating article lines without DOI by title and document type
            dg.drop_duplicates(subset=[lc_title_alias,lc_doc_type_alias],keep='first', inplace=True)
        df_list.append(dg)
    df_articles_concat = pd.concat(df_list)
    
    # Dropping duplicated article lines after merging by titles, document type and journal 
    df_list = []   
    for idx, dg in df_articles_concat.groupby([lc_title_alias,lc_doc_type_alias,norm_journal_alias]): 
        if len(dg)<3:
            # Deduplicating article lines with same title, document type, first author and journal
            # and also with same DOI if not UNKNOWN
            dg[doi_alias] = _find_value_to_keep(dg,doi_alias)
            dg.drop_duplicates(subset=[doi_alias],keep='first',inplace=True)          
        else:   
            # Dropping article lines with DOI UNKNOWN from group of articles with same title, 
            # document type, first author and journal but different DOIs 
            unkown_indices = dg[dg[doi_alias]==UNKNOWN].index
            dg.drop(unkown_indices,inplace=True)
            pub_id_list = [x for x in dg[pub_id_alias]]
            warning = (f'WARNING: Multiple DOI values for same title, document type, first author and journal '
                                  f'are found in the group of article lines with IDs {pub_id_list} '
                                  f'in "_deduplicate_articles" function '
                                  f'called by "parsing_concatenate_deduplicate" function '
                                  f'of "BiblioParsingConcat.py" module.\n'
                                  f'Article lines with DOIs "{UNKNOWN}" has been droped.')                      
            print(Fore.BLUE + warning + Fore.BLACK)
        df_list.append(dg) 
    if df_list != []:
        df_articles_dedup = pd.concat(df_list)
    else:
        df_articles_dedup = df_articles_concat
    df_articles_dedup = df_articles_dedup.drop([lc_title_alias, lc_doc_type_alias], axis = 1)
    df_articles_dedup.sort_values([pub_id_alias], inplace=True)
    
    # Identifying the set of articles IDs to drop in the other parsing files of the concatenated corpus
    pub_id_set_init =  set(df_articles_concat_full[pub_id_alias].to_list())
    pub_id_set_end  =  set(df_articles_dedup[pub_id_alias].to_list())    
    pub_id_to_drop  = pub_id_set_init - pub_id_set_end 
    
    # Setting usefull prints
    articles_nb_init = len(df_articles_concat_full)    
    articles_nb_end  = len(df_articles_dedup)
    articles_nb_drop = articles_nb_init - articles_nb_end
    
    print('\nDeduplication results:')
    print(f'    Initial articles number: {articles_nb_init}')
    print(f'    Final articles number: {articles_nb_end}')
    warning = (f'    WARNING: {articles_nb_drop} articles have been dropped as duplicates')
    print(Fore.BLUE +  bold_text + warning + light_text + Fore.BLACK)
                                
    return (df_articles_dedup, pub_id_to_drop)


def _deduplicate_dat(file_name, pub_id_to_drop, path_in, path_out ):
    
    '''The `deduplicate_dat`function drops the lines corresponding to `pub_id_to_drop' list of articles IDs
    in the '.dat' files issued from concatenation of parsing files of corpuses, exept the one of articles list.
    
    Args : 
       file_name (str): The name of the file among the '.dat' files issued from concatenation of parsing files 
                        of corpuses.
       pub_id_to_drop (list): The list of articles IDs which lines should be dropped from the '.dat' files.
       path_in (path): path of the file file_name.
       path_out (path): path where the modified file file_name is saved.               
        
    Returns :
        None
        
    Notes:
       The globals `COL_NAMES` and `DIC_OUTDIR_PARSING` from 'BiblioSpecificGlobals' module 
       of 'BiblioAnalysis_Utils' package are used.
       
    '''
    
    # 3rd party imports   
    import pandas as pd
    from pathlib import Path
    
    # Local imports
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import COL_NAMES
    from BiblioAnalysis_Utils.BiblioSpecificGlobals import DIC_OUTDIR_PARSING
    
    # Defining aliases for column names of the file_name file 
    pub_id_alias = COL_NAMES['pub_id']
        
    df = pd.read_csv(path_in / Path(file_name), sep="\t")
    
    filt = (df[pub_id_alias].isin(pub_id_to_drop))
    df = df[~filt]
    df.sort_values([pub_id_alias], inplace=True)

    if file_name == DIC_OUTDIR_PARSING['AU']:
        idx_author_alias = COL_NAMES['authors'][1]
        df.sort_values([pub_id_alias,idx_author_alias], inplace=True)
    if file_name == DIC_OUTDIR_PARSING['AD']:
        idx_address_alias = COL_NAMES['address'][1]
        df.sort_values([pub_id_alias,idx_address_alias], inplace=True)        
    if file_name == DIC_OUTDIR_PARSING['CU']:
        idx_address_alias = COL_NAMES['country'][1]
        df.sort_values([pub_id_alias,idx_address_alias], inplace=True)
    if file_name == DIC_OUTDIR_PARSING['I']:
        idx_address_alias = COL_NAMES['institution'][1]
        df.sort_values([pub_id_alias,idx_address_alias], inplace=True) 
    if file_name == DIC_OUTDIR_PARSING['I2']:
        idx_author_alias = COL_NAMES['auth_inst'][1]
        df.sort_values([pub_id_alias,idx_author_alias], inplace=True)
            
    df.to_csv(path_out / Path(file_name),
              index=False,
              sep='\t')

    
