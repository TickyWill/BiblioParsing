__all__ = ['biblio_parser',
           'build_item_df_from_tup',
           'build_title_keywords',
           'check_and_drop_columns',
           'check_and_get_rawdata_file_path',
           'clean_authors_countries_institutions',
           'dict_print',
           'merge_database',
           'normalize_country',
           'normalize_journal_names',
           'normalize_name',
           'rationalize_town_names',
           'remove_special_symbol',
           'set_rawdata_error',
           'upgrade_col_names',
           ]


def dict_print(dic):
    for k,v in dic.items():
        print("            ", k, ":", v)


def check_and_get_rawdata_file_path(rawdata_path, raw_extent):
    '''
    '''
    # Standard library imports
    import os
    from pathlib import Path
    
    # Listing the available files with raw_extent extension
    # ToDo: Management of multiple files to merge with 'merge_database' function
    list_data_base = []
    for path, _, files in os.walk(rawdata_path):
        list_data_base.extend(Path(path) / Path(file) for file in files 
                              if file.endswith(raw_extent))                
    if list_data_base:
        # Selecting the most recent file with raw_extent extension
        list_data_base.sort(key = lambda x: os.path.getmtime(x), reverse=True)
        rawdata_file_path = list_data_base[0]
    else:
        rawdata_file_path = None
    return rawdata_file_path


def set_rawdata_error(database, rawdata_path, raw_extent):
    error_text  = f"\n   !!! No {database} raw-data file available !!! \n"
    error_text += f"\nBefore new launch of the cell, "
    error_text += f"please make available a {database} raw-data file "
    error_text += f"with {raw_extent} extension in:\n   {rawdata_path}."
    return error_text


def build_item_df_from_tup(item_list, item_col_names, item_col, pub_id_alias, dic_failed=None):
    '''Building a clean item dataframe from a tuple 
    and accordingly updating the parsing success rate dict.'''
    
    # Standard library imports
    import pandas as pd
    
    item_df = pd.DataFrame.from_dict({label:[s[idx] for s in item_list] 
                                      for idx,label in enumerate(item_col_names)})
    list_id = item_df[item_df[item_col] == ''][pub_id_alias].values
    list_id = list(set(list_id))
    if dic_failed:
        df_corpus_len = dic_failed['number of article']
        dic_failed[item_col] = {'success (%)':100 * ( 1 - len(list_id) / df_corpus_len),
                                pub_id_alias:[int(x) for x in list(list_id)]}    
    item_df = item_df[item_df[item_col] != '']
    return item_df, dic_failed


def clean_authors_countries_institutions(auth_addr_country_inst_df, verbose=False):
    """
    """
    # 3rd party imports
    import pandas as pd
    
    #Local imports
    from BiblioParsing.BiblioSpecificGlobals import EMPTY
    
    # Setting useful aliases
    empty_alias = EMPTY
    columns_list = auth_addr_country_inst_df.columns
    pub_id_alias = columns_list[0]
    author_alias = columns_list[1]
    address_alias = columns_list[2]
    country_alias = columns_list[3]
    norm_aff_alias = columns_list[4]
    raw_aff_alias = columns_list[5]
    
    new_auth_addr_country_inst_df = pd.DataFrame(columns=columns_list)
    for pub_id, pub_id_dg in auth_addr_country_inst_df.groupby(pub_id_alias):
        new_pub_id_dg = pd.DataFrame(columns=columns_list)
        for author_id, author_dg in pub_id_dg.groupby(author_alias):
            new_author_dg = author_dg.copy()
            if len(author_dg)>1:
                country_list = list(set(author_dg[country_alias].to_list()))
                new_author_dg[country_alias] = "; ".join(country_list)
                
                address_list = author_dg[address_alias].to_list()
                new_author_dg[address_alias] = "; ".join(address_list)
                
                norm_aff_list = list(set(author_dg[norm_aff_alias].to_list()) - {empty_alias})
                new_author_dg[norm_aff_alias] = "; ".join(norm_aff_list)
                
                raw_aff_list = list(set(author_dg[raw_aff_alias].to_list()) - {empty_alias})
                new_author_dg[raw_aff_alias] = "; ".join(raw_aff_list)
                
                new_author_dg.drop_duplicates(subset=[pub_id_alias, author_alias], inplace=True)
                new_pub_id_dg = pd.concat([new_pub_id_dg, new_author_dg])
            else:
                new_pub_id_dg = pd.concat([new_pub_id_dg, author_dg])
        new_auth_addr_country_inst_df = pd.concat([new_auth_addr_country_inst_df, new_pub_id_dg])
    new_auth_addr_country_inst_df.fillna(empty_alias, inplace=True)
    new_auth_addr_country_inst_df.replace("", empty_alias, inplace=True)
    return new_auth_addr_country_inst_df


def build_title_keywords(df):
    
    '''Given the dataframe 'df' with one column 'title':
    
                    Title
            0  Experimental and CFD investigation of inert be...
            1  Impact of Silicon/Graphite Composite Electrode...
            
    the function 'build_title_keywords':
    
       1- Builds the set "keywords_TK" of the tokens appearing at least NOUN_MINIMUM_OCCURRENCE times 
    in all the article titles of the corpus. The tokens are the words of the title with nltk tags 
    belonging to the global list 'NLTK_VALID_TAG_LIST'.
       2- Adds two columns 'token' and 'pub_token' to the dataframe 'df'. The column 'token' contains
    the set of the tokenized and lemmelized (using the nltk WordNetLemmatizer) title. The column
    'pub_token' contains the list of words common to the set "keywords_TK" and to the column 'kept_tokens'
       3- Builds the list of tuples 'list_of_words_occurrences.sort'
    [(token_1,# occurrences token_1), (token_2,# occurrences token_2),...] ordered by decreasing values
    of # occurrences token_i.
       4- Suppress words pertening to BLACKLISTED_WORDS to the list from the bag of words
    
    Args:
       df (dataframe): pub_id | title_alias 

    Returns:
       (tup): tuple (df, bag_of_words_occurrences) with df a dataframe 
       which colums are [pub_id, title_tokens_alias, kept_tokens_alias]  
       where title_tokens_alias contains the list of tokens of the title 
       and kept_tokens_alias the list of tokens with an occurrence frequency 
       >= NOUN_MINIMUM_OCCURRENCES, and bag_of_words_occurrences a list of tuples
       where tuple i is (word_i,# occurrence_i).
        
    '''
    # To Do: update docstring
    
    # Standard library imports
    import operator
    from collections import Counter
       
    # 3rd party library imports
    import nltk
    import numpy as np
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import BLACKLISTED_WORDS
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import NLTK_VALID_TAG_LIST
    from BiblioParsing.BiblioSpecificGlobals import NOUN_MINIMUM_OCCURRENCES
    
    def tokenizer(text):
        
        '''
        Tokenizes, lemmelizes the string 'text'. Only the words with nltk tags in the global
        NLTK_VALID_TAG_LIST are kept.
        
        ex 'Thermal stability of Mg2Si0.55Sn0.45 for thermoelectric applications' 
        gives the list : ['thermal', 'stability', 'mg2si0.55sn0.45', 'thermoelectric', 'application']
        
        Args:
            text (string): string to tokenize
            
        Returns
            The list valid_words_lemmatized 
        '''
            
        tokenized = nltk.word_tokenize(text.lower())
        valid_words = [word for (word, pos) in nltk.pos_tag(tokenized) 
                       if pos in NLTK_VALID_TAG_LIST] 

        stemmer = nltk.stem.WordNetLemmatizer()
        valid_words_lemmatized = [stemmer.lemmatize(valid_word) for valid_word in valid_words]
    
        return valid_words_lemmatized        

    title_alias = COL_NAMES['temp_col'][2]
    title_tokens_alias = COL_NAMES['temp_col'][3]
    kept_tokens_alias = COL_NAMES['temp_col'][4]
    
    df[title_tokens_alias] = df[title_alias].apply(tokenizer)

    bag_of_words = np.array(df[title_tokens_alias].sum()) # remove the blacklisted words from the bag of words
    for remove in BLACKLISTED_WORDS:
        bag_of_words = bag_of_words[bag_of_words != remove] 

    bag_of_words_occurrences = list(Counter(bag_of_words).items())
    bag_of_words_occurrences.sort(key = operator.itemgetter(1),reverse=True)

    keywords_TK = set([x for x,y in bag_of_words_occurrences if y>=NOUN_MINIMUM_OCCURRENCES])
    
    df[kept_tokens_alias] = df[title_tokens_alias].apply(lambda x :list(keywords_TK.intersection(set(x))))
   
    return (df, bag_of_words_occurrences)


def normalize_country(country):
    
    '''
    Normalize the country name for coherence seeking between wos and scopus corpuses.
    '''
    # To Do: update docstring
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import ALIAS_UK
    from BiblioParsing.BiblioGeneralGlobals import COUNTRIES
    from BiblioParsing.BiblioSpecificGlobals import UNKNOWN
    
    country_clean = country
    if country not in COUNTRIES:
        if country in  ALIAS_UK:
            country_clean = 'United Kingdom'
        elif 'USA' in country:
            country_clean = 'United States'
        elif ('china' in country) or ('China' in country):
            country_clean = 'China'
        elif country == 'Russia':    
            country_clean = 'Russian Federation'
        elif country == 'U Arab Emirates':    
            country_clean = 'United Arab Emirates'
        elif country == 'Vietnam':   
            country_clean = 'Viet Nam'
        else:
            country_clean = UNKNOWN

    return country_clean


def merge_database(database,filename,in_dir,out_dir):
    
    '''The `merge_database` function merges several corpus of same database type in one corpus.
    
    Args:
        database (str): database type (scopus or wos).
        filename (str): name of the merged database.
        in_dir (str): name of the folder where the corpuses are saved.
        out_dir (str): name of the folder where the merged corpuses will be saved.
    
    Notes:
        The globals 'SCOPUS' and 'WOS' from `BiblioSpecificGlobals`module 
        of `BiblioParsing`package are used.
        The functions 'read_database_scopus' and 'read_database_wos' 
        from, respectively, `BiblioParsingScopus`module and `BiblioParsingWos` 
        of `BiblioParsing`package are used. 
        
    '''
    # Standard library imports
    import os
    import sys
    from pathlib import Path   

    # 3rd party library imports
    import pandas as pd
    
    # Local library imports
    from BiblioParsing.BiblioParsingScopus import read_database_scopus
    from BiblioParsing.BiblioParsingWos import read_database_wos
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import WOS

    list_data_base = []
    list_df = []
    if database == WOS:
        for path, _, files in os.walk(in_dir):
            list_data_base.extend(Path(path) / Path(file) for file in files
                                                          if file.endswith(".txt"))
        for file in list_data_base:
            list_df.append(read_database_wos(file))

    elif database == SCOPUS:
        for path, _, files in os.walk(in_dir):
            list_data_base.extend(Path(path) / Path(file) for file in files
                                                          if file.endswith(".csv"))
        for file in list_data_base:
            list_df.append(read_database_scopus(file))
    else:
        raise Exception(f"Sorry, unrecognized database {database} : should be {WOS} or {SCOPUS} ")
        
    result = pd.concat(list_df, ignore_index = True)
    result.to_csv(out_dir / Path(filename), sep = '\t')

def normalize_name(text):
    
    '''The `normalize_name` function normalizes the author name spelling according the three debatable rules:
            - replacing none ascii letters by ascii ones,
            - capitalizing first name, 
            - capitalizing surnames,
            - supressing comma and dot.
       It uses the internal funtion `remove_special_symbol`of this module "BiblioParsingUtils".    
       ex: normalize_name(" GrÔŁ-biçà-vèLU D'aillön, E-kj. ")
        >>> "Grol-Bica-Velu D'Aillon E-KJ".
        
    Args:
        text (str): the text to normalize.
    
    Returns
        (str) : The normalized text.
        
    Notes:
        The globals 'DASHES_CHANGE', 'LANG_CHAR_CHANGE' and 'PONCT_CHANGE' 
        from `BiblioGeneralGlobals` module of `BiblioParsing` package are used.
        
    '''

    # Standard library imports
    import re
    
    # Globals imports
    from BiblioParsing.BiblioGeneralGlobals import DASHES_CHANGE    
    from BiblioParsing.BiblioGeneralGlobals import LANG_CHAR_CHANGE
    from BiblioParsing.BiblioGeneralGlobals import PONCT_CHANGE
    
    # Translate special character 
    text = text.translate(DASHES_CHANGE)
    text = text.translate(LANG_CHAR_CHANGE)
    text = text.translate(PONCT_CHANGE)
    
    # Removing accentuated characters
    text = remove_special_symbol(text, only_ascii=True, strip=True)
    
    re_minus = re.compile('(-[a-zA-Z]+)')       # Captures: "cCc-cC-ccc-CCc"
    for text_minus_texts in re.findall(re_minus,text):
        text = text.replace(text_minus_texts,'-' + text_minus_texts[1:].capitalize() )
    
    re_apostrophe = re.compile("('[a-zA-Z]+)")  # Captures: "cCc'cC'ccc'cc'CCc"
    for text_minus_texts in re.findall(re_apostrophe,text):
        text = text.replace(text_minus_texts,"'" + text_minus_texts[1:].capitalize() )
        
    re_minus = re.compile('([a-zA-Z]+-)')       # Captures: "cCc-" 
    for text_minus_texts in re.findall(re_minus,text):
        text = text.replace(text_minus_texts,text_minus_texts[:-1].capitalize() + '-')
        
    re_apostrophe = re.compile("([a-zA-Z]+')")  # Captures: "cCc'"
    for text_minus_texts in re.findall(re_apostrophe,text):
        text = text.replace(text_minus_texts,text_minus_texts[:-1].capitalize() + "'")
        
    re_surname = "[a-zA-Z]+\s"                  # Captures: "cCccC "
    for text_minus_texts in re.findall(re_surname,text):
        text = text.replace(text_minus_texts,text_minus_texts.capitalize())
        
    re_minus_first_name = '\s[a-zA-Z]+-[a-zA-Z]+$'     # Captures: "cCc-cC" in the first name
    for x in  re.findall(re_minus_first_name,text):
        text = text.replace(x,x.upper())
           
    return text


def normalize_journal_names(database,corpus_df):
    '''Tadds the column `normalize_journal_names` to the corpus `corpus_df`. The journal normalized names are expurgated from unecessary
	pieces of information such as : small words defined in a global dict (`DIC_LOW_WORDS`), year, conference edition... These normalized
	and simplified journal names are mainly uses when concatenating two corpus (wos, scpus, ...) using slightly different name for the same
	journal.
    globals.
   
    Args:
        database (string): type of database among the ones defined by SCOPUS and WOS globals.
        corpus_df (dataframe): corpus dataframe to be normalized in terms of journal names.
       
    Returns:
        (dataframe): the dataframe with an additional columns containing the normalized journal names.
       
    Note:
        The globals 'COLUMN_LABEL_WOS', 'COLUMN_LABEL_SCOPUS','DIC_LOW_WORDS', 'RE_YEAR_JOURNAL', 'SCOPUS' and 'WOS' are used.
   
    '''
   
    # Globals imports
    from BiblioParsing.BiblioRegexpGlobals import RE_NUM_CONF
    from BiblioParsing.BiblioRegexpGlobals import RE_YEAR_JOURNAL
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import DIC_LOW_WORDS
    from BiblioParsing.BiblioSpecificGlobals import NORM_JOURNAL_COLUMN_LABEL    
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import WOS
   
   
    import re
    def _journal_normalizer(journal):
        journal = ' ' + journal + ' ' # a lazzy trick to simplefy the regexp
        journal = journal.lower()
        journal = re.sub(RE_YEAR_JOURNAL, ' ', journal)
        journal = re.sub(RE_NUM_CONF, ' ', journal)
        for old_str, new_str in DIC_LOW_WORDS.items():
            journal = journal.replace(old_str, new_str)
        journal = re.sub('\s+',' ',journal)
        journal = journal.strip()
        return journal

    if database == WOS:
        journal_alias = COLUMN_LABEL_WOS['journal']
    elif database == SCOPUS:
        journal_alias = COLUMN_LABEL_SCOPUS['journal']
    else:
        raise Exception(f"Sorry, unrecognized database {database}: should be {WOS} or {SCOPUS} ")
    
    norm_journal_alias = NORM_JOURNAL_COLUMN_LABEL
    corpus_df[norm_journal_alias] = corpus_df[journal_alias].apply(_journal_normalizer)
    
    return corpus_df


def biblio_parser(rawdata_path, database, inst_filter_list = None,
                  country_affiliations_file_path = None,
                  inst_types_file_path = None,
                  country_towns_file = None,
                  country_towns_folder_path = None):
    
    '''The `biblio_parser` function parse wos or scopus databases using the appropriate parser.
    
    Args:
    
    
    Returns:
        (dict): The dict of dataframes resulting from the parsing.
    '''
    
    # Local library imports
    from BiblioParsing.BiblioParsingScopus import biblio_parser_scopus
    from BiblioParsing.BiblioParsingWos import biblio_parser_wos
    
    # Globals imports
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import WOS
    
    if database == WOS:
        wos_tup = biblio_parser_wos(rawdata_path, inst_filter_list = inst_filter_list,
                                   country_affiliations_file_path = country_affiliations_file_path,
                                   inst_types_file_path = inst_types_file_path,
                                   country_towns_file = country_towns_file,
                                   country_towns_folder_path = country_towns_folder_path)
        parsing_dict, dic_failed = wos_tup[0], wos_tup[1]
    elif database == SCOPUS:
        scopus_tup = biblio_parser_scopus(rawdata_path, inst_filter_list = inst_filter_list,
                                          country_affiliations_file_path = country_affiliations_file_path,
                                          inst_types_file_path = inst_types_file_path,
                                          country_towns_file = country_towns_file,
                                          country_towns_folder_path = country_towns_folder_path)
        parsing_dict, dic_failed = scopus_tup[0], scopus_tup[1]
    else:
        raise Exception(f"Sorry, unrecognized database {database} : should be wos or scopus ")
        
    return parsing_dict, dic_failed

        
def check_and_drop_columns(database, df):
    # Standard libraries import
    import numpy as np
    
    # Local imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS 
    from BiblioParsing.BiblioSpecificGlobals import COLUMN_LABEL_WOS_PLUS    
    from BiblioParsing.BiblioSpecificGlobals import SCOPUS
    from BiblioParsing.BiblioSpecificGlobals import WOS
    
    
    # Setting useful aliases
    wos_col_issn_alias  = COLUMN_LABEL_WOS["issn"]
    wos_col_eissn_alias = COLUMN_LABEL_WOS_PLUS["e_issn"]
    pub_id_col_alias    = COL_NAMES["pub_id"] 

    # Check for missing mandatory columns
    if database == WOS:
        cols_mandatory = set([val for val in COLUMN_LABEL_WOS.values() if val] + [COLUMN_LABEL_WOS_PLUS["e_issn"]])
    elif database == SCOPUS:
        cols_mandatory = set([val for val in COLUMN_LABEL_SCOPUS.values() if val])    
    else:
        raise Exception(f"Sorry, unrecognized database {database} : should be {WOS} or {SCOPUS} ")
        
    cols_available = set(df.columns)
    missing_columns = cols_mandatory.difference(cols_available)
    if missing_columns:
        error_text  = f'The mandarory columns: {",".join(missing_columns)} are missing '
        error_text += f'in rawdata extracted from {database}.\nPlease correct before proceeding.'
        raise Exception(error_text)
    
    # Setting issn to e_issn if issn not available for wos
    if database == WOS:
        df = df.replace('',np.nan,regex=True) # To allow the use of combine_first
        df[wos_col_issn_alias] = df[wos_col_issn_alias].combine_first(df[wos_col_eissn_alias])
        df = df.dropna(axis = 0, how = 'all')
        cols_mandatory = set([val for val in COLUMN_LABEL_WOS.values() if val])
        
        
    # Columns selection and dataframe reformatting    
    cols_to_drop = list(cols_available.difference(cols_mandatory))
    df.drop(cols_to_drop,
            axis=1,
            inplace=True)                    # Drops unused columns    
    df.index = range(len(df))                # Sets the pub_id in df index
    df = df.rename_axis(pub_id_col_alias).reset_index() # Sets the pub-id as a column
    return df

                    
def upgrade_col_names(corpus_folder):
    
    '''Add names to the colummn of the parsing and filter_<i> files to take into account the
    upgrage of BiblioParsing.
    
    Args:
        corpus_folder (str): folder of the corpus to be adapted
        
    Notes:
        The global 'COL_NAMES' from `BiblioSpecificGlobals` module 
        of `BiblioParsing` package are used.
    
    '''
    # Standard library imports
    import os
    
    # 3rd party library imports
    import pandas as pd
    from pandas.core.groupby.groupby import DataError
    
    # Local imports
    from BiblioParsing.BiblioSpecificGlobals import COL_NAMES
    
    # Beware: the new file authorsinst.dat is not present in the old parsing folders
    dict_filename_conversion  = {'addresses.dat':'address',
                                 'articles.dat': 'articles',
                                 'authors.dat':'authors',
                                 'authorsinst.dat':'auth_inst',
                                 'authorskeywords.dat':'keywords',
                                 'countries.dat':'country',
                                 'institutions.dat':'institution',
                                 'journalkeywords.dat':'keywords',
                                 'references.dat':'references',
                                 'subjects.dat': 'subject',
                                 'subjects2.dat':'sub_subject',
                                 'titlekeywords.dat':'keywords'}

    for dirpath, dirs, files in os.walk(corpus_folder):  
        if ('parsing' in   dirpath) |  ('filter_' in  dirpath):
            for file in  [file for file in files
                          if (file.split('.')[1]=='dat') 
                          and (file!='database.dat')      # Not used this file is no longer generated
                          and (file!='keywords.dat') ]:   # Not used this file is no longer generated
                try:
                    df = pd.read_csv(os.path.join(dirpath,file),sep='\t',header=None)
                    
                    if df.loc[0].tolist() == COL_NAMES[dict_filename_conversion[file]]:
                        print(f'The file {os.path.join(dirpath,file)} is up to date')
                    else:
                        df.columns = COL_NAMES[dict_filename_conversion[file]]
                        df.to_csv(os.path.join(dirpath,file),sep='\t',index=False)
                        print(f'*** The file {os.path.join(dirpath,file)} has been upgraded ***')
                except  pd.errors.EmptyDataError:
                    df = pd.DataFrame(columns=COL_NAMES[dict_filename_conversion[file]])
                    df.to_csv(os.path.join(dirpath,file),sep='\t',index=False)
                    print(f'*** The EMPTY file {os.path.join(dirpath,file)} has been upgraded ***')
                except:
                    print(f'Warning: File {os.path.join(dirpath,file)} not recognized as a parsing file')


def rationalize_town_names(text, dic_town_symbols = None, dic_town_words = None):
    """The function `rationalize_town_names` replaces in the string 'text'
    symbols and words defined by the keys of the dictionaries 'DIC_TOWN_SYMBOLS'
    and 'DIC_TOWN_WORDS' by their corresponding values in these dictionaries.
    
    Args:
        text (str): The string where changes will be done.
        
    Returns:
        (str): The modified string.
        
    Notes:
        The globals 'DIC_TOWN_SYMBOLS' and 'DIC_TOWN_WORDS' are imported from
        `BiblioSpecificGlobals` module of `BiblioParsing' package.
    """    
    if dic_town_symbols == None: 
        # Globals imports
        from BiblioParsing.BiblioSpecificGlobals import DIC_TOWN_SYMBOLS    
        dic_town_symbols = DIC_TOWN_SYMBOLS
    if dic_town_words == None: 
        # Globals imports
        from BiblioParsing.BiblioSpecificGlobals import DIC_TOWN_WORDS
        dic_town_words = DIC_TOWN_WORDS
    
    # Uniformizing symbols in town names using the dict 'DIC_TOWN_SYMBOLS'
    for town_symb in dic_town_symbols.keys():
        text = text.replace(town_symb, dic_town_symbols[town_symb])

    # Uniformizing words in town names using the dict 'DIC_TOWN_WORDS'
    for town_word in dic_town_words.keys():
        text = text.replace(town_word, dic_town_words[town_word])    
    return text

def remove_special_symbol(text, only_ascii = True, strip = True):
    '''The function `remove_special_symbol` removes accentuated characters in the string 'text'
    and ignore non-ascii characters if 'only_ascii' is true. Finally, spaces at the ends of 'text'
    are removed if strip is true.
    
    Args:
        text (str): The text where to remove special symbols.
        only_ascii (boolean): If True, non-ascii characters are removed from 'text' (default: True).
        strip (boolean): If True, spaces at the ends of 'text' are removed (default: True).
        
    Returns:
        (str): The modified string 'text'.
    
    '''
    # Standard library imports
    import functools
    import unicodedata

    if only_ascii:
        nfc = functools.partial(unicodedata.normalize,'NFD')
        text = nfc(text). \
                   encode('ascii', 'ignore'). \
                   decode('utf-8')
    else:
        nfkd_form = unicodedata.normalize('NFKD',text)
        text = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

    if strip:
        text = text.strip()
    
    return text

################################# Deprecated functions ##########################################
# country_normalization deprecated, replaced by "normalize_country"
# town_names_uniformization deprecated, replaced by "rationalize_town_names"
# accent_remove deprecated, replaced by "remove_special_symbol"
# special_symbol_remove deprecated, replaced by "remove_special_symbol"

